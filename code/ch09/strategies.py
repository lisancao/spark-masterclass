"""Chapter 9: join types, the five join strategies, how Spark selects one, and what each costs.

Timings: run under the bench lock.
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch09/strategies.py
"""
import contextlib
import fcntl
import os
import re
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.errors import PySparkException
from pyspark.sql import functions as F
from common.book import session, plan, TRIPS, ZONES, DATA

spark = session("ch09-strategies", spark_sql_session_timeZone="UTC")
trips = spark.read.parquet(str(TRIPS))
zones = spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))
trips.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")


def err(e):
    first = next(l for l in str(e).splitlines() if l.strip())
    return f"{e.getCondition()}: " + first.split("] ", 1)[-1][:70]


def join_ops(df):
    return re.findall(r"\b(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin|BroadcastNestedLoopJoin|"
                      r"CartesianProduct)\b", plan(df, "simple"))


@contextlib.contextmanager
def bench_lock():
    """Hold the shared bench lock only while a timing runs (the book quotes these timings).
    BOOK_NO_LOCK=1 skips the lock, for films and test runs whose timings the book does not quote."""
    if os.environ.get("BOOK_NO_LOCK"):
        yield
        return
    with open(DATA / ".bench.lock", "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def median_s(build, runs=3):
    """Median wall time of build().collect() over `runs` runs after one warmup, a new DataFrame each run."""
    with bench_lock():
        build().collect()
        ts = []
        for _ in range(runs):
            t0 = time.perf_counter()
            build().collect()
            ts.append(time.perf_counter() - t0)
    return statistics.median(ts)


# 1. Join types on a tiny example.
print("== 1. join types")
a = spark.createDataFrame([(1, "a1"), (2, "a2"), (3, "a3"), (None, "a_null")], "k INT, a STRING")
b = spark.createDataFrame([(2, "b2"), (3, "b3"), (3, "b3x"), (4, "b4"), (None, "b_null")], "k INT, b STRING")
for how in ("inner", "left", "right", "full", "left_semi", "left_anti"):
    j = a.join(b, "k", how)
    rows = j.orderBy(*[F.col(c).asc_nulls_first() for c in j.columns])
    print(f"  {how:<9} {[tuple(r) for r in rows.collect()]}")
print("  cross    ", a.crossJoin(b).count(), "rows")
print("  k = k    ", a.join(b, a.k == b.k).count(), "rows | k <=> k:", a.join(b, a.k.eqNullSafe(b.k)).count(), "rows")
try:
    a.join(b, a.k == b.k).select("k").collect()
except PySparkException as e:
    print("  select('k') after a.k == b.k:", err(e))
print("  columns after join on 'k':", a.join(b, "k").columns, "| on a.k == b.k:", a.join(b, a.k == b.k).columns)

# 2. The default plan for trips x zones, and why.
print("\n== 2. the default strategy")
j = trips.join(zones, trips.PULocationID == zones.LocationID)
stats = zones._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
print("zones estimated size:", str(stats), "bytes | threshold:",
      spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
print("trips x zones ->", join_ops(j))
print("\n".join(l[:100] for l in plan(j, "simple").splitlines()[1:9]))

# 3. The same join with each strategy forced by a hint, and the time of each.
print("\n== 3. trips x zones, each strategy (count per borough; median of 3 after a warmup)")
Q = """SELECT /*+ {hint} */ z.Borough, count(*) AS n
       FROM trips t JOIN zones z ON t.PULocationID {op} z.LocationID GROUP BY z.Borough"""
cases = [("BROADCAST(z)", "="), ("SHUFFLE_HASH(z)", "="), ("MERGE(z)", "="), ("SHUFFLE_REPLICATE_NL(z)", "=")]
for hint, op in cases:
    sql = Q.format(hint=hint, op=op)
    df = spark.sql(sql)
    print(f"  {hint:<24} {join_ops(df)}  {median_s(lambda: spark.sql(sql)):.2f} s")

# 4. A non-equi join: no keys to hash or sort on.
print("\n== 4. a non-equi join")
bands = spark.createDataFrame([("short", 0.0, 15.0), ("medium", 15.0, 40.0), ("long", 40.0, 1e9)],
                              "band STRING, lo DOUBLE, hi DOUBLE")
nj = trips.join(bands, (trips.fare_amount >= bands.lo) & (trips.fare_amount < bands.hi))
print("fare in [lo, hi) ->", join_ops(nj), f"{median_s(lambda: nj.groupBy('band').count()):.2f} s")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
nj2 = trips.join(bands, (trips.fare_amount >= bands.lo) & (trips.fare_amount < bands.hi))
print("same, broadcast off ->", join_ops(nj2), f"{median_s(lambda: nj2.groupBy('band').count()):.2f} s")
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

# 5. Two large sides: every trip joined to a per-trip table on a trip id.
print("\n== 5. two large sides (3.8 M x 3.8 M rows on a trip id)")
left = trips.select(F.col("_metadata.row_index").alias("trip_id"), "PULocationID", "fare_amount")
right = trips.select(F.col("_metadata.row_index").alias("trip_id"), "tip_amount")
for hint in ("", "broadcast", "shuffle_hash", "merge"):
    def build(h=hint):
        r = right.hint(h) if h else right
        return left.join(r, "trip_id").agg(F.sum("tip_amount"))
    print(f"  {hint or 'no hint':<13} {join_ops(build())}  {median_s(build):.2f} s")

# 6. AQE changes a sort-merge join into a broadcast join when a side turns out small.
print("\n== 6. AQE changes the strategy at run time")
busy = spark.sql("""SELECT PULocationID, count(*) AS n FROM trips GROUP BY PULocationID HAVING count(*) > 100000""")
aj = trips.join(busy, "PULocationID").agg(F.count("*"))
print("before it runs:", join_ops(aj))
aj.collect()
final = plan(aj, "simple")
print("after it runs: ", re.findall(r"\b(BroadcastHashJoin|SortMergeJoin)\b", final.split("== Initial Plan ==")[0]))
print([l.strip()[:80] for l in final.splitlines() if "AQEShuffleRead" in l or "BroadcastQueryStage" in l][:3])
spark.conf.set("spark.sql.adaptive.enabled", "false")
aj2 = trips.join(busy, "PULocationID").agg(F.count("*"))
print("AQE off:", join_ops(aj2), f"{median_s(lambda: trips.join(busy, 'PULocationID').agg(F.count('*'))):.2f} s")
spark.conf.unset("spark.sql.adaptive.enabled")
print("AQE on: ", f"{median_s(lambda: trips.join(busy, 'PULocationID').agg(F.count('*'))):.2f} s")

# 7. A hint that cannot apply.
print("\n== 7. a hint that cannot apply")
fj = spark.sql("SELECT /*+ BROADCAST(t) */ count(*) FROM trips t FULL JOIN zones z ON t.PULocationID = z.LocationID")
print("BROADCAST(t) on a FULL join ->", join_ops(fj))
