"""Chapter 42: what cache() does to a plan, what it costs, and the traps around it.

Classic only (Storage REST endpoints, df.rdd). Run from code/:  uv run python ch42/caching.py
"""
import shutil, statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark import StorageLevel
from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TAXI, MONTH_START, MONTH_END

# 4g: with the default 1g heap and 16 task slots, building the cache of six months failed with
# OutOfMemoryError in one run (see the part notes).
spark = session("ch42-caching", spark_sql_session_timeZone="UTC", spark_driver_memory="4g",
                spark_ui_showConsoleProgress="false")
H1 = str(TAXI / "yellow-h1")


def cleaned():
    """A fresh DataFrame each call, built the same way: six months of trips, cleaned."""
    t = spark.read.parquet(H1)
    return t.where((F.col("fare_amount") > 0) & F.col("trip_distance").between(0.1, 100)) \
            .select("tpep_pickup_datetime", "PULocationID", "DOLocationID", "fare_amount", "tip_amount",
                    "trip_distance", "payment_type")


clean = cleaned()
print("before cache():", clean.storageLevel, "| is_cached", clean.is_cached)
clean.cache()
print("after cache(): ", clean.storageLevel, "| is_cached", clean.is_cached)
print("cache() is lazy: storage entries now:", len(ui_api("storage/rdd")))

q = clean.where(F.col("PULocationID") == 132).agg(F.avg("fare_amount"))
print("\nthe plan of a query on the cached DataFrame:")
for l in plan(q, "simple").splitlines():
    if "InMemory" in l or "FileScan" in l:
        print("  " + l.strip()[:150])

t = time.perf_counter()
n = clean.count()
fill = time.perf_counter() - t
info = ui_api("storage/rdd")[0]
print(f"\nfirst action fills the cache: {n:,} rows in {fill:.1f} s")
print(f"Storage tab: '{info['name'][:60]}...'")
print(f"  {info['numCachedPartitions']}/{info['numPartitions']} partitions cached, "
      f"memory {info['memoryUsed'] / 2**20:,.0f} MiB, disk {info['diskUsed'] / 2**20:,.0f} MiB, "
      f"level '{info['storageLevel']}'")


def median_ms(build, runs=7):
    """Median wall time of collect() on a new DataFrame from build(), after one warm-up run.

    Each run builds a new DataFrame. A second collect() on the same DataFrame object reuses the
    shuffle files of the first one, and the scheduler skips those stages (see below).
    """
    build().collect()
    ts = []
    for _ in range(runs):
        df = build()
        t = time.perf_counter(); df.collect(); ts.append(time.perf_counter() - t)
    return statistics.median(ts) * 1000


query = lambda d: d.groupBy("PULocationID").agg(F.avg("tip_amount"), F.count("*"))
other = lambda: spark.read.parquet(H1).where(F.col("fare_amount") > 0)
print("\na cheap aggregate, median of 7 runs after a warm-up:")
print(f"  from the cache             {median_ms(lambda: query(clean)):6.0f} ms")
print(f"  a new DataFrame, same plan {median_ms(lambda: query(cleaned())):6.0f} ms   (cache matched by plan)")
print(f"  a different plan           {median_ms(lambda: query(other())):6.0f} ms   (reads the Parquet files)")
print("  same plan, InMemoryTableScan:", "InMemoryTableScan" in plan(query(cleaned()), "simple"),
      "| different plan:", "InMemoryTableScan" in plan(query(other()), "simple"))


def routes(d):
    """An expensive step: one row per route and hour, after a shuffle of all trips."""
    return (d.withColumn("hour", F.date_trunc("hour", "tpep_pickup_datetime"))
            .groupBy("PULocationID", "DOLocationID", "hour")
            .agg(F.count("*").alias("n"), F.sum("fare_amount").alias("fares")))


small = lambda d: d.where(F.col("n") > 20).agg(F.sum("fares"))
print(f"\nan expensive step (a shuffle into {routes(clean).count():,} route-hours), then a small query on it:")
print(f"  recomputed each time       {median_ms(lambda: small(routes(clean)), runs=5):6.0f} ms")
again = small(routes(clean))
again.collect()
t = time.perf_counter(); again.collect(); second = (time.perf_counter() - t) * 1000
label = "same object again"
spark.sparkContext.setJobDescription(label)
again.collect()
skipped = sum(len(j.get("skippedStages", [])) if isinstance(j.get("skippedStages"), list) else j.get("numSkippedStages", 0)
              for j in ui_api("jobs") if j.get("description") == label)
spark.sparkContext.setJobDescription(None)
print(f"  the same DataFrame object, collect() again {second:6.0f} ms   (skipped stages: {skipped})")
heavy = routes(clean).cache()
heavy.count()
print(f"  from a cache of the step   {median_ms(lambda: small(routes(clean)), runs=5):6.0f} ms   (InMemoryTableScan: "
      f"{'InMemoryTableScan' in plan(small(routes(clean)), 'simple')})")
heavy.unpersist()

# A second persist() with another level does nothing.
clean.persist(StorageLevel.DISK_ONLY)
print("\nafter persist(DISK_ONLY) on the cached DataFrame:", clean.storageLevel)
clean.unpersist()
print("after unpersist():", clean.storageLevel, "| storage entries:", len(ui_api("storage/rdd")))

# SQL: CACHE TABLE is eager, CACHE LAZY TABLE waits.
cleaned().createOrReplaceTempView("clean_trips")
spark.sql("CACHE LAZY TABLE clean_trips")
print("\nCACHE LAZY TABLE: storage entries", len(ui_api("storage/rdd")),
      "| cached partitions", sum(r["numCachedPartitions"] for r in ui_api("storage/rdd")))
spark.sql("UNCACHE TABLE clean_trips")
t = time.perf_counter()
spark.sql("CACHE TABLE clean_trips")
print(f"CACHE TABLE took {time.perf_counter() - t:.1f} s: storage entries", len(ui_api("storage/rdd")),
      "| name", ui_api("storage/rdd")[0]["name"])
spark.catalog.clearCache()
print("after clearCache():", len(ui_api("storage/rdd")), "entries")

# Storage levels available from Python.
print("\nPySpark storage levels:")
for name in sorted(n for n in dir(StorageLevel) if n.isupper()):
    lvl = getattr(StorageLevel, name)
    print(f"  {name:<24} disk={lvl.useDisk!s:<5} memory={lvl.useMemory!s:<5} offHeap={lvl.useOffHeap!s:<5} "
          f"deserialized={lvl.deserialized!s:<5} replicas={lvl.replication}")

# A DataFrame keeps the file list it made when it was created.
inbox = Path("/tmp/masterclass-ch42/inbox")
shutil.rmtree(inbox, ignore_errors=True); inbox.mkdir(parents=True)
shutil.copy(TAXI / "yellow-h1" / "yellow_tripdata_2026-01.parquet", inbox)
df = spark.read.parquet(str(inbox))
shutil.copy(TAXI / "yellow-h1" / "yellow_tripdata_2026-02.parquet", inbox)
print("\na file arrives after read.parquet():")
print(f"  old DataFrame counts {df.count():,} rows; a new read counts {spark.read.parquet(str(inbox)).count():,}")
spark.catalog.refreshByPath(str(inbox))
print(f"  after refreshByPath: old DataFrame counts {df.count():,}")
(inbox / "yellow_tripdata_2026-01.parquet").unlink()
try:
    df.count()
except Exception as e:
    import re
    cond = re.search(r"\[([A-Z_.]+)\]", str(e))
    print("  a listed file is deleted:", cond.group(1) if cond else str(e).splitlines()[0][:100])
