"""Chapter 19: repartition and coalesce, what each does to the plan, and what coalesce(1) does upstream.

Classic for getNumPartitions; the spark_partition_id count works on Connect too.
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch19/repartition.py
"""
import statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TAXI

spark = session("ch19-repartition")
trips = spark.read.parquet(str(TAXI / "yellow-h1"))     # six months, 6 files
print("input partitions:", trips.rdd.getNumPartitions())

VARIANTS = [
    ("repartition(8)", lambda d: d.repartition(8)),
    ("repartition(8, 'PULocationID')", lambda d: d.repartition(8, "PULocationID")),
    ("repartition('PULocationID')", lambda d: d.repartition("PULocationID")),
    ("repartitionByRange(4, 'fare_amount')", lambda d: d.repartitionByRange(4, "fare_amount")),
    ("coalesce(4)", lambda d: d.coalesce(4)),
    ("coalesce(100)", lambda d: d.coalesce(100)),
]
print(f"\n{'call':<38}{'partitions':>11}  top of the plan")
for label, f in VARIANTS:
    df = f(trips)
    top = next(l.strip("+- ").strip() for l in plan(df, "simple").splitlines()[1:]
               if "Exchange" in l or "Coalesce" in l)
    print(f"{label:<38}{df.rdd.getNumPartitions():>11}  {top[:62]}")

# A partition count that also works on Spark Connect (it counts only non-empty partitions).
df = trips.repartition("PULocationID")
print("\nnon-empty partitions after repartition('PULocationID'):",
      df.select(F.spark_partition_id().alias("p")).distinct().count())

# coalesce(1) has no stage boundary, so the work before it runs in one task too.
expensive = trips.select(F.sha2(F.concat_ws("|", *trips.columns), 256).alias("h"))
out = "/tmp/masterclass-ch19/hashes"


def timed(label, df, runs=5):
    spark.sparkContext.setJobDescription(label)
    df.write.mode("overwrite").format("noop").save(out)          # warm-up
    times = []
    for _ in range(runs):
        t = time.perf_counter()
        df.write.mode("overwrite").format("noop").save(out)
        times.append(time.perf_counter() - t)
    stages = [s for s in ui_api("stages?status=complete") if s.get("description") == label]
    tasks = sorted({s["numTasks"] for s in stages})
    print(f"  {label:<26} median {statistics.median(times):5.2f} s  (task counts per stage: {tasks})")


print(f"\nSHA-256 of every row of {trips.count():,} trips, written to the noop sink, 5 runs after a warm-up:")
timed("no change", expensive)
timed("coalesce(1)", expensive.coalesce(1))
timed("repartition(1)", expensive.repartition(1))
