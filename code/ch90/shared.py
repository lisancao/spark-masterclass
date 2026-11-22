"""Chapter 90: closures, what gets serialized, broadcast variables and accumulators, and their DataFrame equivalents.

Classic only. Run from code/:  uv run python ch90/shared.py
"""
import sys
import threading
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark import cloudpickle
from pyspark.sql import Observation, functions as F
from common.book import session, TRIPS, ZONES

spark = session("ch90-shared", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
trips = spark.read.parquet(str(TRIPS))
ids = trips.select("PULocationID").rdd.map(lambda r: r[0]).cache()
ids.count()

print("== 1. a closure gets a copy of the variable, not the variable")
counter = 0


def bump(x):
    global counter
    counter += 1


ids.foreach(bump)
print("counter after foreach over", f"{ids.count():,}", "ids:", counter)

print("\n== 2. what the closure takes with it")
zone_names = {int(l.split(",")[0].strip('"')): l.split(",")[2].strip('"')
              for l in open(ZONES).read().splitlines()[1:]}
big = {i: f"value {i:>9} " + "x" * 80 for i in range(200_000)}   # 200,000 distinct strings
small_fn = lambda z: zone_names.get(z, "?")
big_fn = lambda z: big.get(z, zone_names.get(z, "?"))
print(f"pickled function with zone_names ({len(zone_names)} entries): {len(cloudpickle.dumps(small_fn)):>12,} bytes")
print(f"pickled function with big dict ({len(big):,} entries): {len(cloudpickle.dumps(big_fn)):>12,} bytes")

threshold = sc._jvm.PythonUtils.getBroadcastThreshold(sc._jsc)
print(f"spark.broadcast.UDFCompressionThreshold: {threshold:,} bytes (above it, PySpark broadcasts the command)")
small_ids = ids.coalesce(16).cache(); small_ids.count()


def jobs(make_fn, n=5):
    t = time.perf_counter()
    for _ in range(n):
        small_ids.map(make_fn()).count()
    return (time.perf_counter() - t) / n


in_closure = jobs(lambda: big_fn)
bbig = sc.broadcast(big)
by_bc = jobs(lambda: (lambda z: bbig.value.get(z, "?")))
print(f"5 jobs, big dict in the closure: {in_closure:.2f} s per job;  as one broadcast: {by_bc:.2f} s per job")
bbig.destroy()

try:
    lock = threading.Lock()
    ids.map(lambda z: (lock, z)).count()
except Exception as e:
    print("closure with a threading.Lock:", type(e).__name__, str(e).splitlines()[0][:70])
try:
    ids.map(lambda z: sc.defaultParallelism).count()
except Exception as e:
    print("closure that uses sc:", type(e).__name__, "|", [l for l in str(e).splitlines() if "SparkContext" in l][0][:78])

print("\n== 3. a broadcast variable: one copy per executor, read with .value")
bc = sc.broadcast(zone_names)
named = ids.map(lambda z: bc.value.get(z, "?"))
print("type:", type(bc).__name__, " id:", bc._jbroadcast.id(), " top zone:",
      named.map(lambda n: (n, 1)).reduceByKey(lambda a, b: a + b).max(key=lambda kv: kv[1]))
bc.unpersist()                                                # drop executor copies, keep the driver copy
print("after unpersist(), value in driver:", len(bc.value), "entries")
bc.destroy()                                                  # remove it everywhere
try:
    ids.map(lambda z: bc.value.get(z)).count()
except Exception as e:
    msg = [l for l in str(e).splitlines() if l.startswith("Caused by: org.apache")][0]
    print("after destroy():", msg.split("Exception: ")[1].split(" (destroy")[0])

print("\n== 4. accumulators: tasks add, only the driver reads")
bad = sc.accumulator(0)
total = sc.accumulator(0.0)


def check(row):
    if row.fare_amount is None or row.fare_amount <= 0:
        bad.add(1)
    else:
        total.add(row.fare_amount)


trips.select("fare_amount").rdd.foreach(check)
print(f"fares <= 0 or null: {bad.value:,}   sum of good fares: {total.value:,.2f}")
twice = sc.accumulator(0)
m = ids.map(lambda z: (twice.add(1), z)[1])                   # an update inside a transformation
m.count(); m.count()
print(f"accumulator inside map(), two actions on an uncached RDD: {twice.value:,} (= 2 x {ids.count():,})")

print("\n== 5. the DataFrame equivalents")
zones_df = spark.read.option("header", True).csv(str(ZONES))
j = trips.join(F.broadcast(zones_df), trips.PULocationID == zones_df.LocationID)
print("F.broadcast(df) join:", [l.strip() for l in j._jdf.queryExecution().executedPlan().toString().splitlines()
                                if "Broadcast" in l][0][:70])
lookup = F.create_map(*[x for k, v in sorted(zone_names.items())[:3] for x in (F.lit(k), F.lit(v))])
print("a small map as a literal column:", trips.select(lookup[F.col("PULocationID")].alias("z"))
      .where("z IS NOT NULL").first())
obs = Observation("fares")
counted = trips.observe(obs, F.count_if(F.col("fare_amount") <= 0).alias("bad"),
                        F.sum(F.when(F.col("fare_amount") > 0, F.col("fare_amount"))).alias("total"))
counted.write.format("noop").mode("overwrite").save()
print("Observation after one action:", {k: round(v, 2) for k, v in obs.get.items()})
