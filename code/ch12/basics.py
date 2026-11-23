"""Chapter 12: the RDD API as it stands in 4.3: create, transform, act, and the key-value operations.

Classic only (RDDs are not in Spark Connect).
Run from code/:  uv run python ch12/basics.py
"""
import sys
import time
from operator import add
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, TRIPS, ZONES

spark = session("ch12-basics", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
here = str(Path(__file__).resolve().parent) + "/"

print("== 1. two ways to make an RDD")
nums = sc.parallelize(range(1, 1_000_001), 8)
lines = sc.textFile(str(ZONES))
print("parallelize:", nums.getNumPartitions(), "partitions,", type(nums).__name__)
print("textFile:   ", lines.getNumPartitions(), "partitions, first line:", lines.first())

print("\n== 2. transformations are lazy, actions run jobs")
tracker = spark._jsparkSession.sparkContext().statusTracker()
jobs = lambda: len(tracker.getJobIdsForGroup(None))
before = jobs()
t = time.perf_counter()
evens = nums.filter(lambda x: x % 2 == 0).map(lambda x: x * x)
print(f"filter + map defined in {1000 * (time.perf_counter() - t):.2f} ms, jobs run: {jobs() - before}")
print("count():", evens.count(), " jobs run:", jobs() - before)
print("take(3):", evens.take(3), " reduce(add):", evens.reduce(add))

print("\n== 3. the zone file as key-value pairs")
header = lines.first()
zones = (lines.filter(lambda l: l != header)
         .map(lambda l: l.replace('"', "").split(","))
         .map(lambda f: (f[1], f[2])))                        # (borough, zone)
print("first pairs:", zones.take(3))
per_borough = zones.mapValues(lambda z: 1).reduceByKey(add).sortBy(lambda kv: -kv[1])
print("reduceByKey:", per_borough.collect())

print("\n== 4. trips as pairs: (PULocationID, fare)")
trips = spark.read.parquet(str(TRIPS)).select("PULocationID", "fare_amount", "tip_amount")
pairs = trips.rdd.map(lambda r: (r.PULocationID, r.fare_amount))
print("an RDD from a DataFrame:", pairs.getNumPartitions(), "partitions")

# aggregateByKey: a zero value, a merge within a partition, a merge across partitions
sum_count = pairs.aggregateByKey((0.0, 0), lambda acc, v: (acc[0] + v, acc[1] + 1),
                                 lambda a, b: (a[0] + b[0], a[1] + b[1]))
avg = sum_count.mapValues(lambda s: round(s[0] / s[1], 2))
print("aggregateByKey avg fare, zones 132, 138, 161:",
      sorted(avg.filter(lambda kv: kv[0] in (132, 138, 161)).collect()))

# combineByKey: the general form of the three
mx = pairs.combineByKey(lambda v: v, max, max)
print("combineByKey max fare, zone 132:", mx.filter(lambda kv: kv[0] == 132).values().collect())
try:
    mx.lookup(132)                                            # calls the partitioner in the driver
except Exception as e:
    print("lookup() in this driver:", e.getCondition())

names = (zones.zipWithIndex().map(lambda zi: (zi[1] + 1, zi[0][1])))   # LocationID is the line number
top = (pairs.mapValues(lambda v: 1).reduceByKey(add)
       .join(names).map(lambda kv: (kv[1][0], kv[1][1])).sortByKey(False))
print("join with zone names, top 3:", top.take(3))
print("countByKey is an action that returns a dict:", type(pairs.countByKey()).__name__)

print("\n== 5. the lineage of that last RDD")
print(top.toDebugString().decode().replace(here, ""))
