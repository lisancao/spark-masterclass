"""Chapter 9: a skewed key in a sort-merge join, what AQE does about it, salting, and buckets.

Timings: run under the bench lock.
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch09/skew.py
"""
import contextlib
import fcntl
import os
import re
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TRIPS, DATA

WH = tempfile.mkdtemp(prefix="ch09-")
spark = session("ch09-skew", spark_sql_session_timeZone="UTC", spark_sql_warehouse_dir=WH,
                spark_sql_autoBroadcastJoinThreshold="-1")
trips = spark.read.parquet(str(TRIPS))


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


def join_stage(desc):
    """Task time and shuffle-read quantiles of the stage that ran the join, for the job with this description."""
    time.sleep(2)
    jobs = [j for j in ui_api("jobs") if j.get("description") == desc]
    stage_ids = sorted({s for j in jobs for s in j["stageIds"]})
    best = None
    for sid in stage_ids:
        for st in ui_api(f"stages/{sid}"):
            if st["status"] == "COMPLETE" and st["shuffleReadRecords"] > 0 and st["numTasks"] > 1:
                if best is None or st["shuffleReadRecords"] > best["shuffleReadRecords"]:
                    best = st
    q = ui_api(f"stages/{best['stageId']}/{best['attemptId']}/taskSummary?quantiles=0.5,1.0")
    run = q["executorRunTime"]
    recs = q["shuffleReadMetrics"]["readRecords"]
    return best["numTasks"], run, recs


# 1. The skew in real data: pickups per zone.
print("== 1. skew in the trips")
per_zone = trips.groupBy("PULocationID").count()
s = per_zone.agg(F.max("count"), F.expr("percentile(count, 0.5)"), F.count("*")).first()
print(f"zones: {s[2]}, median pickups per zone: {s[1]:,.0f}, largest zone: {s[0]:,}")

# 2. A synthetic join with one hot key: 40% of 30 M rows have key 0.
print("\n== 2. a sort-merge join with one hot key")
N, KEYS = 30_000_000, 200_000
facts = spark.range(N).select(F.when(F.col("id") % 10 < 4, F.lit(0)).otherwise(F.col("id") % KEYS).alias("k"),
                              (F.col("id") % 97).alias("v"))
dims = spark.range(KEYS).select(F.col("id").alias("k"), (F.col("id") * 3).alias("w"))
WORK = Path(WH) / "skew"
facts.write.parquet(str(WORK / "facts"))
dims.write.parquet(str(WORK / "dims"))
facts = spark.read.parquet(str(WORK / "facts"))
dims = spark.read.parquet(str(WORK / "dims"))


def skew_join():
    return facts.join(dims, "k").agg(F.sum(F.col("v") + F.col("w")).alias("s"))


spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16MB")
for label, on in (("AQE skew join off", "false"), ("AQE skew join on", "true")):
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", on)
    spark.sparkContext.setJobDescription(label)
    res = skew_join().first().s
    spark.sparkContext.setJobDescription(None)
    tasks, run, recs = join_stage(label)
    print(f"  {label:<18} sum={res}  join stage: {tasks} tasks, task time median {run[0]:.0f} ms, "
          f"max {run[1]:.0f} ms, rows read median {recs[0]:,.0f}, max {recs[1]:,.0f}")
    t = median_s(skew_join)
    print(f"  {'':<18} median of 3: {t:.2f} s")
q = skew_join()
q.collect()
fp = plan(q, "simple")
print("  final plan:", [l.strip()[:84] for l in fp.split("== Initial Plan ==")[0].splitlines()
                         if "SortMergeJoin" in l or "AQEShuffleRead" in l][:3])

# 3. Salting: spread the hot key over S sub-keys, and copy each dimension row S times.
print("\n== 3. salting with AQE skew join off")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
S = 16


def salted():
    f = facts.withColumn("salt", F.when(F.col("k") == 0, (F.rand(7) * S).cast("int")).otherwise(F.lit(0)))
    d = dims.withColumn("salt", F.explode(F.when(F.col("k") == 0, F.sequence(F.lit(0), F.lit(S - 1)))
                                          .otherwise(F.array(F.lit(0)))))
    return f.join(d, ["k", "salt"]).agg(F.sum(F.col("v") + F.col("w")).alias("s"))


spark.sparkContext.setJobDescription("salted")
print("  sum:", salted().first().s)
spark.sparkContext.setJobDescription(None)
tasks, run, recs = join_stage("salted")
print(f"  join stage: {tasks} tasks, task time median {run[0]:.0f} ms, max {run[1]:.0f} ms, "
      f"rows read median {recs[0]:,.0f}, max {recs[1]:,.0f}")
print(f"  median of 3: {median_s(salted):.2f} s")
spark.conf.unset("spark.sql.adaptive.skewJoin.enabled")

# 4. A hot key that should not join at all: NULL keys.
print("\n== 4. NULL keys")
nulls = spark.range(N).select(F.when(F.col("id") % 10 < 4, F.lit(None)).otherwise(F.col("id") % KEYS)
                              .cast("bigint").alias("k"), (F.col("id") % 97).alias("v"))
nj = nulls.join(dims, "k")
print("  inner join on a key that is 40% NULL, filter in the plan:",
      [l.strip()[:60] for l in plan(nj, "simple").splitlines() if "isnotnull(k" in l][:1])

# 5. Buckets: write both sides bucketed on the key, and the join needs no shuffle.
print("\n== 5. bucketed tables")
small_trips = trips.select("PULocationID", "fare_amount")
small_trips.write.bucketBy(16, "PULocationID").sortBy("PULocationID").saveAsTable("trips_b")
(trips.groupBy("PULocationID").agg(F.avg("fare_amount").alias("zone_avg"))
 .write.bucketBy(16, "PULocationID").saveAsTable("zone_avg_b"))
def bucketed():
    return spark.table("trips_b").join(spark.table("zone_avg_b"), "PULocationID") \
        .agg(F.sum(F.col("fare_amount") - F.col("zone_avg")))


def plain():
    return small_trips.join(trips.groupBy("PULocationID").agg(F.avg("fare_amount").alias("zone_avg")),
                            "PULocationID").agg(F.sum(F.col("fare_amount") - F.col("zone_avg")))


bj, nb = bucketed(), plain()
bp = plan(bj, "simple")
print("  Exchange operators:", bp.count("Exchange"), "| join:", re.findall(r"\b(SortMergeJoin)\b", bp)[:1])
print("  scan:", [l.strip()[:90] for l in bp.splitlines() if "SelectedBucketsCount" in l][:1])
print("  the same join on plain DataFrames, Exchange operators:", plan(nb, "simple").count("Exchange"))
print(f"  bucketed: {median_s(bucketed):.2f} s | not bucketed: {median_s(plain):.2f} s")
shutil.rmtree(WH, ignore_errors=True)
