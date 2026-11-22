"""Chapter 37: build MinHash with banding, and an IVF index, from DataFrame functions. Measure recall against work.

Part 1 finds reposted listings (character 5-shingles, Jaccard similarity).
Part 2 searches 20,000 clustered unit vectors with an IVF index built on MLlib KMeans.
Run from code/:  uv run python ch37/indexes.py
"""
import random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.ml.functions import array_to_vector
from pyspark.sql import functions as F
from common.book import session, plan, ROOT

P = 2147483647          # 2^31 - 1: (a * x + b) stays below 2^62, so BIGINT math cannot overflow
SHINGLE = 5
spark = session("ch37-indexes", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="8")
spark.sparkContext.setLogLevel("OFF")
listings = spark.read.json(str(ROOT / "code" / "data" / "messy" / "listings" / "listings.jsonl"))

# ---- Part 1: shingles, exact Jaccard, MinHash, bands
text = F.trim(F.regexp_replace(F.lower("description"), r"[^a-z0-9]+", " "))
grams = F.when(F.length(text) < SHINGLE, F.array(text)).otherwise(
    F.transform(F.sequence(F.lit(1), F.length(text) - SHINGLE + 1), lambda i: F.substring(text, i, SHINGLE)))
sh = listings.select("listing_id", F.transform(F.array_distinct(grams),
                                                lambda s: F.pmod(F.xxhash64(s), F.lit(P))).alias("h")).cache()
n = sh.count()
all_pairs = n * (n - 1) // 2
print(f"== {n} listings, {all_pairs:,} pairs")
print("  sequence(1, 0) =", spark.range(1).select(F.sequence(F.lit(1), F.lit(0))).first()[0],
      " (counts down, which is why short text needs its own branch)")

a, b = sh.alias("a"), sh.alias("b")
jac = F.size(F.array_intersect("a.h", "b.h")) / F.size(F.array_union("a.h", "b.h"))
exact = (a.join(b, F.col("a.listing_id") < F.col("b.listing_id"))
         .select(F.col("a.listing_id").alias("x"), F.col("b.listing_id").alias("y"), jac.alias("j"))).cache()
exact.count()
print(f"  exact all-pairs Jaccard: {all_pairs:,} pairs; plan join:",
      [op for op in ("BroadcastNestedLoopJoin", "CartesianProduct") if op in plan(exact, "simple")])
exact.select(F.floor(F.col("j") * 10).alias("tenth")).groupBy("tenth").count().orderBy("tenth") \
    .select((F.col("tenth") / 10).alias("j from"), "count").show()

# An inverted index on shingles: join on equal shingle values.
inv = sh.select("listing_id", F.explode("h").alias("s"))
joined = inv.alias("p").join(inv.alias("q"), (F.col("p.s") == F.col("q.s")) &
                             (F.col("p.listing_id") < F.col("q.listing_id")))
print(f"  inverted index on shingles: {joined.count():,} join rows "
      f"for {joined.select('p.listing_id', 'q.listing_id').distinct().count():,} distinct pairs")

K, T = 100, 0.7
rng = random.Random(7)
params = [(rng.randrange(1, P), rng.randrange(0, P)) for _ in range(K)]
coeffs = F.array(*[F.struct(F.lit(x).cast("bigint").alias("a"), F.lit(y).cast("bigint").alias("b"))
                   for x, y in params])
sig = sh.withColumn("sig", F.transform(coeffs, lambda c: F.array_min(
    F.transform("h", lambda x: (c["a"] * x + c["b"]) % P)))).cache()
sig.count()
print("\n== MinHash signatures: one Project, no Python, no shuffle")
sig_plan = plan(sh.withColumn("sig", F.transform(coeffs, lambda c: F.array_min(
    F.transform("h", lambda x: (c["a"] * x + c["b"]) % P)))).select("sig"), "simple")
print("  operators above the cached shingles:", [l.strip().lstrip("+- ").split(" ")[0].split("(")[0]
                                              for l in sig_plan.splitlines()[1:] if l.strip()][:3],
      "| Exchange:", "Exchange" in sig_plan, "| Python:", "EvalPython" in sig_plan)
try:
    spark.range(1).select((F.lit(2 ** 61 - 1).cast("bigint") * F.lit(1000)).alias("x")).first()
except Exception as e:
    print("  BIGINT overflow under ANSI:", str(e).split("]")[0] + "]")

rows = {r.listing_id: r.sig for r in sig.collect()}
errs = []
for r in exact.where("j > 0").collect():
    est = sum(p == q for p, q in zip(rows[r.x], rows[r.y])) / K
    errs.append((r.j, abs(est - r.j)))
print(f"  estimate error over {len(errs):,} pairs with J > 0, K={K}:")
for lo, hi in ((0, .3), (.3, .6), (.6, .9), (.9, 1.01)):
    e = [d for j, d in errs if lo <= j < hi]
    mid = (lo + min(hi, 1)) / 2
    if e:
        print(f"    J in [{lo:.1f}, {min(hi, 1):.1f}): mean |error| {np.mean(e):.4f}"
              f"   theory sd at J={mid:.2f}: {np.sqrt(mid * (1 - mid) / K):.4f}   pairs {len(e):,}")

truth = {(r.x, r.y) for r in exact.where(f"j >= {T}").collect()}
print(f"\n== banding: {len(truth)} pairs with J >= {T}")
print("   b   r  threshold  P(cand | J=0.7)  candidates  of all pairs  recall")
for b_, r_ in ((100, 1), (50, 2), (25, 4), (20, 5), (10, 10)):
    bands = sig.select("listing_id", F.posexplode(F.transform(
        F.sequence(F.lit(0), F.lit(b_ - 1)), lambda i: F.xxhash64(F.slice("sig", i * r_ + 1, r_))))
        .alias("band", "bucket"))
    x, y = bands.alias("x"), bands.alias("y")
    cands = (x.join(y, (F.col("x.band") == F.col("y.band")) & (F.col("x.bucket") == F.col("y.bucket"))
                    & (F.col("x.listing_id") < F.col("y.listing_id")))
             .select(F.col("x.listing_id").alias("x"), F.col("y.listing_id").alias("y")).distinct())
    got = {(r.x, r.y) for r in cands.collect()}
    print(f"  {b_:>3} {r_:>3}  {(1 / b_) ** (1 / r_):>9.3f}  {1 - (1 - T ** r_) ** b_:>15.4f}  {len(got):>10,}"
          f"  {len(got) / all_pairs:>11.2%}  {len(truth & got) / len(truth):.3f}")
    if (b_, r_) == (25, 4):
        print("     band join plan:", [op for op in ("SortMergeJoin", "BroadcastHashJoin", "Generate")
                                       if op in plan(cands, "simple")])
        verified = cands.join(exact, ["x", "y"]).where(f"j >= {T}").count()
        print(f"     after exact verification: {verified} pairs, precision 1.0 by construction")

# MLlib MinHashLSH: each hash table is one band of one row (OR only).
print("\n== MLlib MinHashLSH (OR-amplification only)")
words = sh.select("listing_id", F.transform("h", lambda v: v.cast("string")).alias("toks"))
feat = HashingTF(inputCol="toks", outputCol="f", numFeatures=1 << 20, binary=True).transform(words)
for tables in (5, 25):
    m = MinHashLSH(inputCol="f", outputCol="mh", numHashTables=tables, seed=7).fit(feat)
    pairs = (m.approxSimilarityJoin(feat, feat, 1.0 - 0.0001)
             .where("datasetA.listing_id < datasetB.listing_id")
             .select(F.col("datasetA.listing_id").alias("x"), F.col("datasetB.listing_id").alias("y")))
    got = {(r.x, r.y) for r in pairs.collect()}
    print(f"  numHashTables={tables:<3} candidates {len(got):>7,} ({len(got) / all_pairs:.1%} of pairs), "
          f"recall {len(truth & got) / len(truth):.3f}")

# ---- Part 2: IVF over clustered vectors
print("\n== IVF")
rs = np.random.default_rng(48)
dim, clusters, N, NQ = 32, 40, 20000, 200
centers = rs.normal(size=(clusters, dim))
w = rs.zipf(1.6, size=clusters).astype(float)
lab = rs.choice(clusters, size=N + NQ, p=w / w.sum())
Xall = centers[lab] + rs.normal(scale=0.6, size=(N + NQ, dim))
Xall /= np.linalg.norm(Xall, axis=1, keepdims=True)
X, Q = Xall[:N], Xall[N:]
sc = spark.sparkContext
vectors = spark.createDataFrame(sc.parallelize([(i, X[i].tolist()) for i in range(N)], 4),
                                "id int, v array<double>").cache()
queries = spark.createDataFrame(sc.parallelize([(i, Q[i].tolist()) for i in range(NQ)], 2),
                                "qid int, q array<double>").cache()
try:
    spark.createDataFrame([(0, X[0])], "id int, v array<double>").count()
except Exception as e:
    print("  createDataFrame with a numpy array:", str(e).split("]")[0] + "]")

NLIST = 64
km = KMeans(k=NLIST, seed=1, maxIter=20, featuresCol="f").fit(vectors.select(array_to_vector("v").alias("f")))
print(f"  KMeans k={NLIST} on {N:,} vectors of {dim} values, {km.summary.numIter} iterations")
C = [list(map(float, c)) for c in km.clusterCenters()]


def sqdist(u, v):
    return F.aggregate(F.zip_with(u, v, lambda p, q: (p - q) * (p - q)), F.lit(0.0), lambda s, z: s + z)


cent = F.array(*[F.array(*[F.lit(x) for x in c]) for c in C])
d = F.transform(cent, lambda c: sqdist(F.col("v"), c))
index = vectors.withColumn("list", F.array_position(d, F.array_min(d)) - 1).cache()
sizes = sorted((r["count"] for r in index.groupBy("list").count().collect()), reverse=True)
print(f"  list sizes: largest {sizes[:5]}, smallest {sizes[-3:]}, mean {np.mean(sizes):.0f}")

exact_nn = np.argsort(((Q[:, None, :] - X[None, :, :]) ** 2).sum(-1), axis=1)[:, :10]
dq = F.transform(cent, lambda c: sqdist(F.col("q"), c))
ranked = F.array_sort(F.arrays_zip(dq.alias("d"), F.sequence(F.lit(0), F.lit(NLIST - 1)).alias("l")))
print("  nprobe  vectors scored  of brute force  recall@10")
for nprobe in (1, 2, 4, 8, 16, 64):
    probes = queries.select("qid", "q", F.explode(F.slice(ranked, 1, nprobe)).alias("p")) \
        .select("qid", "q", F.col("p.l").alias("list"))
    scored = probes.join(index, "list").select("qid", "id", sqdist(F.col("q"), F.col("v")).alias("dist"))
    top = scored.groupBy("qid").agg(F.transform(F.slice(F.array_sort(F.collect_list(F.struct("dist", "id"))), 1, 10),
                                                lambda s: s["id"]).alias("ids"))
    res = {r.qid: r.ids for r in top.collect()}
    work = scored.count()
    rec = np.mean([len(set(res.get(q, [])) & set(exact_nn[q])) / 10 for q in range(NQ)])
    print(f"  {nprobe:>6}  {work:>14,}  {work / (N * NQ):>14.1%}  {rec:>9.3f}")

# The same top 10 with NEAREST BY: brute force, every query against every vector.
vectors.createOrReplaceTempView("vecs")
queries.createOrReplaceTempView("qs")
nb = spark.sql("""SELECT q.qid, v.id FROM qs q JOIN vecs v
                  EXACT NEAREST 10 BY DISTANCE aggregate(zip_with(q.q, v.v, (x, y) -> (x - y) * (x - y)), 0D, (s, z) -> s + z)""")
got = {}
for r in nb.collect():
    got.setdefault(r.qid, set()).add(r.id)
rec = np.mean([len(got[q] & set(exact_nn[q])) / 10 for q in range(NQ)])
print(f"  NEAREST 10 BY DISTANCE: {N * NQ:,} distances, recall@10 {rec:.3f}")
print("  plan:", [op for op in ("BroadcastNestedLoopJoin", "CartesianProduct", "SortAggregate", "ObjectHashAggregate",
                                "Generate") if op in plan(nb, "simple")])
