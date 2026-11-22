"""Chapter 38: connected components at scale. Label propagation against large-star / small-star,
and what an iterative loop does to the plan when nothing cuts it.

Classic only (plan sizes come from df._jdf).
Run from code/:  uv run python ch38/components.py
"""
import random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window, functions as F
from common.book import session, plan, DATA

spark = session("ch38-components", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="8")
spark.sparkContext.setLogLevel("OFF")
spark.sparkContext.setCheckpointDir(str(DATA / "ch38" / "checkpoints"))
ONLY_LOOP = sys.argv[1:] == ["loop"]      # the film runs only the last two sections


def clean(raw):
    return (raw.where(F.col("src").isNotNull() & F.col("dst").isNotNull() & (F.col("src") != F.col("dst")))
            .select(F.least("src", "dst").alias("src"), F.greatest("src", "dst").alias("dst")).distinct())


def large_star(e):
    both = e.select(F.explode(F.array(F.struct(F.col("src").alias("u"), F.col("dst").alias("v")),
                                      F.struct(F.col("dst").alias("u"), F.col("src").alias("v")))).alias("x")
                    ).select("x.u", "x.v")
    m = F.least(F.min("v").over(Window.partitionBy("u")), F.col("u"))
    return both.withColumn("m", m).where("v > u").select(F.col("v").alias("src"), F.col("m").alias("dst")).distinct()


def small_star(e):
    d = e.select(F.greatest("src", "dst").alias("u"), F.least("src", "dst").alias("v")) \
        .withColumn("m", F.min("v").over(Window.partitionBy("u")))
    out = d.select(F.explode(F.array(F.struct(F.col("v").alias("src"), F.col("m").alias("dst")),
                                     F.struct(F.col("u").alias("src"), F.col("m").alias("dst")))).alias("x")
                   ).select("x.src", "x.dst")
    return out.where("src != dst").distinct()


def converged(e):
    most = e.groupBy("src").count().agg(F.max("count")).first()[0]
    if most is not None and most > 1:
        return False
    return e.select(F.col("dst").alias("n")).intersect(e.select(F.col("src").alias("n"))).limit(1).count() == 0


def star_components(raw, trace=False):
    e = clean(raw).localCheckpoint()
    r = 0
    while True:
        r += 1
        e = small_star(large_star(e)).localCheckpoint()
        if trace:
            print(f"     after round {r}:", sorted((x.src, x.dst) for x in e.collect()))
        if converged(e):
            return e, r


def propagation(raw, max_rounds=200):
    e = clean(raw)
    both = e.unionByName(e.select(F.col("dst").alias("src"), F.col("src").alias("dst"))).localCheckpoint()
    lab = both.select(F.col("src").alias("id")).distinct().withColumn("label", F.col("id")).localCheckpoint()
    for r in range(1, max_rounds + 1):
        offer = both.join(lab.withColumnRenamed("id", "src"), "src").groupBy(F.col("dst").alias("id")) \
            .agg(F.min("label").alias("o"))
        new = lab.join(offer, "id", "left").select("id", F.least("label", F.coalesce("o", "label")).alias("label")) \
            .localCheckpoint()
        if new.join(lab.withColumnRenamed("label", "old"), "id").where("label != old").limit(1).count() == 0:
            return new, r
        lab = new
    return lab, max_rounds


def path(n):
    return spark.createDataFrame([(i, i + 1) for i in range(1, n)], "src long, dst long")


def random_graph(nodes=5000, edges=3000, seed=38):
    rng = random.Random(seed)
    return spark.createDataFrame([(rng.randrange(nodes), rng.randrange(nodes)) for _ in range(edges)],
                                 "src long, dst long")


if not ONLY_LOOP:
  print("== large-star and small-star on a path of 6 nodes")
  _, r = star_components(path(6), trace=True)
  print(f"   converged after {r} rounds: every node points at node 1")

  print("\n== rounds and time: label propagation against large-star / small-star")
  print("   graph                 propagation          stars")
  for name, g in (("path of 50", path(50)), ("path of 200", path(200)),
                  ("random, 5,000 nodes", random_graph())):
      t0 = time.time(); _, r1 = propagation(g); t1 = time.time() - t0
      t0 = time.time(); _, r2 = star_components(g); t2 = time.time() - t0
      print(f"   {name:<20} {r1:>4} rounds {t1:>6.1f} s   {r2:>3} rounds {t2:>5.1f} s")

  g = random_graph()
  e, _ = star_components(g)
  nodes = clean(g).select(F.explode(F.array("src", "dst")).alias("id")).distinct()
  comp = nodes.join(e.select(F.col("src").alias("id"), F.col("dst").alias("c")), "id", "left") \
      .select("id", F.coalesce("c", "id").alias("c"))


  def union_find(edges):
      parent = {}
      def find(x):
          parent.setdefault(x, x)
          while parent[x] != x:
              parent[x] = parent[parent[x]]
              x = parent[x]
          return x
      for a, b in edges:
          ra, rb = find(a), find(b)
          if ra != rb:
              parent[max(ra, rb)] = min(ra, rb)
      return {x: find(x) for x in parent}


  ref = union_find([(r.src, r.dst) for r in clean(g).collect()])
  got = {r.id: r.c for r in comp.collect()}
  print(f"   random graph: {len(got):,} nodes in {len(set(got.values())):,} components, "
        f"same as a driver union-find: {got == ref}")
  print("   exchanges in one large-star round over cleaned edges (cleanup distinct, window, distinct):",
        plan(large_star(clean(g)), "simple").count("Exchange"), "exchanges")

print("\n== the plan of an iterative loop: four ways to keep the next round")
g = path(64)


def loop(mode, rounds=6):
    e = clean(g)
    if mode != "nothing":
        e = e.localCheckpoint()
    rows = []
    for r in range(1, rounds + 1):
        nxt = small_star(large_star(e))
        if mode == "cache":
            nxt = nxt.cache(); nxt.count()
        elif mode == "localCheckpoint":
            nxt = nxt.localCheckpoint()
        elif mode == "checkpoint":
            nxt = nxt.checkpoint()
        qe = nxt._jdf.queryExecution()
        t0 = time.time()
        nxt.count()
        rows.append((r, qe.analyzed().collectLeaves().length(),
                     qe.optimizedPlan().toString().count("\n"), time.time() - t0))
        e = nxt
    return rows


for mode in ("nothing", "cache", "localCheckpoint", "checkpoint"):
    rows = loop(mode, rounds=5 if mode == "nothing" else 6)
    print(f"   {mode}:")
    print("     round  leaves  optimized plan lines  count() s")
    for r, leaves, lines, secs in rows:
        print(f"     {r:>5}  {leaves:>6}  {lines:>20}  {secs:>9.2f}")
spark.catalog.clearCache()

lc = clean(g).localCheckpoint()
print("\n== what each cut leaves in the plan")
print("   localCheckpoint():", plan(lc, "simple").splitlines()[1].strip()[:70])
cp = clean(g).checkpoint()
print("   checkpoint():     ", plan(cp, "simple").splitlines()[1].strip()[:70])
files = sorted(p.name for p in (DATA / "ch38" / "checkpoints").rglob("*") if p.is_dir())[:3]
print("   checkpoint() wrote under spark.checkpoint dir:", len(list((DATA / "ch38" / "checkpoints").rglob("part-*"))),
      "part files")
