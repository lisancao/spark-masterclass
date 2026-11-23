"""Chapter 57: mergeable summaries. HLL, theta and KLL sketches over a week of messy app events,
stored as BINARY columns, rolled up after the raw data is gone.

Run from code/:  uv run python ch57/sketches.py
"""
import bisect, json, math, os, random, shutil, statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, DATA, TRIPS

ROOT = DATA / "ch57"
RAW, SK = ROOT / "events", ROOT / "sketches"
shutil.rmtree(ROOT, ignore_errors=True)
spark = session("ch57-sketches", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="8")
spark.sparkContext.setLogLevel("OFF")

# ---- a week of events: users return from day to day; ids arrive in upper case, with spaces, empty or NULL
rng = random.Random(50)
COUNTRIES = ["CA", "US", "VN", "FR", "BR"]
pool = [f"u{n:05d}" for n in range(40000)]
home = {u: rng.choice(COUNTRIES) for u in pool}
users_by_day, latencies, prev = {}, [], set()
for day in range(7):
    active = {u for u in sorted(prev) if rng.random() < 0.45} | set(rng.sample(pool, 9000))
    (RAW / f"day={day}").mkdir(parents=True)
    with open(RAW / f"day={day}" / "events.jsonl", "w") as f:
        for u in sorted(active):
            users_by_day.setdefault(day, set()).add(u)
            for _ in range(rng.randint(1, 5)):
                roll = rng.random()
                raw = u.upper() if roll < 0.08 else f"  {u} " if roll < 0.12 else u
                ms = round(rng.lognormvariate(4.0, 0.8), 2)
                latencies.append(ms)
                f.write(json.dumps({"user": raw, "country": home[u], "latency_ms": ms}) + "\n")
        for _ in range(300):
            ms = round(rng.lognormvariate(4.0, 0.8), 2)
            latencies.append(ms)
            f.write(json.dumps({"user": rng.choice(["", None]), "country": rng.choice(COUNTRIES),
                                "latency_ms": ms}) + "\n")
    prev = active
week = set().union(*users_by_day.values())
events = spark.read.schema("user string, country string, latency_ms double").json(str(RAW))
print(f"== {events.count():,} events over 7 days; exact distinct users: week {len(week):,}, "
      f"daily {[len(users_by_day[d]) for d in range(7)]}")

# ---- Step 1: distinct counts do not add
daily_exact = events.groupBy("day").agg(F.count_distinct(F.lower(F.trim("user"))).alias("d"))
print(f"   sum of daily distinct counts: {daily_exact.agg(F.sum('d')).first()[0]:,}  (the week has {len(week):,})")

# ---- Step 2: the raw id is messy
print("\n== the same person, three spellings")
raw_ids = events.select(F.count_distinct("user").alias("raw"),
                        F.count_distinct(F.lower(F.trim("user"))).alias("normalized")).first()
print(f"   distinct raw ids {raw_ids.raw:,}, after lower(trim()) {raw_ids.normalized:,} "
      "(the empty string still counts as one id)")
user = F.when(F.lower(F.trim("user")) == "", None).otherwise(F.lower(F.trim("user")))
probe = spark.createDataFrame([("a",), ("",), (None,), ("A",)], "u string")
print("   hll_sketch_agg over 'a', '', NULL, 'A':",
      probe.select(F.hll_sketch_estimate(F.hll_sketch_agg("u"))).first()[0],
      "| count(DISTINCT u):", probe.select(F.count_distinct("u")).first()[0])
mixed = spark.createDataFrame([(1, "1"), (2, "2")], "n bigint, s string")
print("   one sketch over BIGINT 1, 2 and STRING '1', '2':",
      mixed.select(F.hll_sketch_estimate(F.hll_union(F.hll_sketch_agg("n"), F.hll_sketch_agg("s")))).first()[0])

# ---- Step 3: approx_count_distinct (HLL++) error against its parameter
print("\n== approx_count_distinct (HyperLogLog++)")
uid = F.lower(F.trim("user"))
for rsd in (0.05, 0.01):
    p = math.ceil(2 * math.log2(1.106 / rsd))
    est = events.where(user.isNotNull()).select(F.approx_count_distinct(uid, rsd)).first()[0]
    print(f"   rsd {rsd}: p = {p}, m = 2^{p} = {2 ** p:,} registers, true rsd 1.04/sqrt(m) = "
          f"{1.04 / math.sqrt(2 ** p):.4f}; estimate {est:,} (error {(est - len(week)) / len(week):+.3%})")
print("   plan:", [l.strip()[:90] for l in plan(events.groupBy("day").agg(F.approx_count_distinct(uid)),
                                                "simple").splitlines() if "Aggregate" in l][:2])

# measured relative error over many groups
groups = spark.range(200 * 20000).select((F.col("id") % 200).alias("g"), F.col("id").alias("v"))
for rsd in (0.05, 0.01):
    ests = [r.e for r in groups.groupBy("g").agg(F.approx_count_distinct("v", rsd).alias("e")).collect()]
    rel = [(e - 20000) / 20000 for e in ests]
    print(f"   200 groups of 20,000 distinct values, rsd {rsd}: measured rsd {statistics.pstdev(rel):.4f}, "
          f"worst {max(map(abs, rel)):.3%}")

# ---- Step 4: daily sketches as BINARY columns, then delete the raw data
sk = (events.groupBy("day", "country")
      .agg(F.hll_sketch_agg(user, 14).alias("users_hll"),
           F.theta_sketch_agg(user, 12).alias("users_theta"),
           F.kll_sketch_agg_double("latency_ms", 200).alias("latency_kll"),
           F.count("*").alias("events")))
sk.write.parquet(str(SK))
size_raw = sum(p.stat().st_size for p in RAW.rglob("*.jsonl"))
size_sk = sum(p.stat().st_size for p in SK.rglob("*.parquet"))
print(f"\n== sketch table: {sk.count()} rows; raw JSON {size_raw / 1e6:.1f} MB, sketches {size_sk / 1e6:.2f} MB")
print("   schema:", ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in spark.read.parquet(str(SK)).schema))
shutil.rmtree(RAW)
sketches = spark.read.parquet(str(SK))
print("   raw events deleted; the rollups read only:", [Path(p).name[:20] for p in sketches.inputFiles()][:2], "...")

# ---- Step 5: rollups from sketches
print("\n== distinct users from merged HLL sketches")
wk = sketches.agg(F.hll_sketch_estimate(F.hll_union_agg("users_hll")).alias("u")).first().u
print(f"   week: estimate {wk:,}, exact {len(week):,}, error {(wk - len(week)) / len(week):+.3%}")
by_c = {r.country: r.u for r in sketches.groupBy("country").agg(
    F.hll_sketch_estimate(F.hll_union_agg("users_hll")).alias("u")).collect()}
for c in COUNTRIES:
    ex = sum(1 for u in week if home[u] == c)
    print(f"   {c}: estimate {by_c[c]:>6,}  exact {ex:>6,}  error {(by_c[c] - ex) / ex:+.3%}")
print("   plan of the rollup:", [l.strip()[:80] for l in plan(sketches.agg(F.hll_union_agg("users_hll")),
                                                            "simple").splitlines() if "Aggregate" in l][:2])
try:
    spark.createDataFrame([(1,)], "x int").select(F.hll_sketch_agg("x", 12).alias("s")).crossJoin(
        spark.createDataFrame([(2,)], "x int").select(F.hll_sketch_agg("x", 14).alias("t"))) \
        .select(F.hll_union("s", "t")).first()
except Exception as e:
    print("   hll_union of lgConfigK 12 and 14:", str(e).split("]")[0] + "]")
print("   with allowDifferentLgConfigK=True:", spark.createDataFrame([(1,)], "x int")
      .select(F.hll_sketch_agg("x", 12).alias("s"), F.hll_sketch_agg("x", 14).alias("t"))
      .select(F.hll_sketch_estimate(F.hll_union("s", "t", True))).first()[0])

print("\n== theta: users seen on two days (intersection)")
for da, db in ((0, 1), (0, 6)):
    a = sketches.where(f"day = {da}").agg(F.theta_union_agg("users_theta").alias("a"))
    b = sketches.where(f"day = {db}").agg(F.theta_union_agg("users_theta").alias("b"))
    est = a.crossJoin(b).select(F.theta_sketch_estimate(F.theta_intersection("a", "b"))).first()[0]
    union = a.crossJoin(b).select(F.theta_sketch_estimate(F.theta_union("a", "b"))).first()[0]
    ex = len(users_by_day[da] & users_by_day[db])
    exu = len(users_by_day[da] | users_by_day[db])
    print(f"   days {da} and {db}: intersection estimate {est:>6,} exact {ex:>6,} "
          f"error {est - ex:+5d} ({(est - ex) / ex:+.1%}) | union {union:,} exact {exu:,} "
          f"| intersection is {ex / exu:.1%} of the union")

print("\n== KLL: latency quantiles for the week from 35 daily sketches")
merged = sketches.agg(F.kll_merge_agg_double("latency_kll").alias("k"))
ranks = (0.5, 0.9, 0.99, 0.999)
row = merged.select(F.kll_sketch_get_n_double("k").alias("n"),
                    *[F.kll_sketch_get_quantile_double("k", F.lit(r)).alias(f"q{i}") for i, r in enumerate(ranks)]).first()
srt = sorted(latencies)
print(f"   n = {row.n:,} values (exact {len(srt):,})")
print("   rank    estimate     exact   rank of the estimate")
for i, r in enumerate(ranks):
    est = row[f"q{i}"]
    exact = srt[min(len(srt) - 1, int(r * len(srt)))]
    print(f"   {r:<6} {est:>9.2f} {exact:>9.2f}   {bisect.bisect_right(srt, est) / len(srt):.4f}")
exact_pa = spark.createDataFrame([(x,) for x in latencies[:200000]], "v double")
print("   percentile_approx is deterministic:",
      len({exact_pa.select(F.percentile_approx("v", 0.99)).first()[0] for _ in range(3)}) == 1)

print("\n== count-min sketch")
cms = spark.read.parquet(str(TRIPS)).select(F.count_min_sketch(F.col("PULocationID"), F.lit(0.001), F.lit(0.99),
                                                               F.lit(42)).alias("cms")).first().cms
print(f"   count_min_sketch over 3.8 M pickup zones: {len(cms):,} bytes")
top = (spark.read.parquet(str(TRIPS)).select(F.expr("approx_top_k(PULocationID, 3)").alias("t")).first().t)
print("   approx_top_k(PULocationID, 3):", [(r.item, r["count"]) for r in top])
