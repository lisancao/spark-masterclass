"""Chapter 90: partitioners. Where a key goes, what partitionBy keeps, and a join that does not shuffle again.

Classic only. Run from code/ with a fixed hash seed (the driver calls portable_hash):
  PYTHONHASHSEED=0 uv run python ch90/partitions.py
"""
import sys
from operator import add
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.rdd import portable_hash
from common.book import session, TRIPS

spark = session("ch90-partitions", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
here = str(Path(__file__).resolve().parent) + "/"

trips = spark.read.parquet(str(TRIPS)).select("PULocationID", "fare_amount", "tip_amount")
fares = trips.rdd.map(lambda r: (r.PULocationID, r.fare_amount or 0.0))
tips = trips.rdd.map(lambda r: (r.PULocationID, r.tip_amount or 0.0))

print("== 1. a new RDD has no partitioner")
print("fares.partitioner:", fares.partitioner)

print("\n== 2. partitionBy(8): key -> portable_hash(key) % 8")
for k in (132, 138, 161, 237):
    print(f"  key {k}: portable_hash = {portable_hash(k)}, partition {portable_hash(k) % 8}")
by_zone = fares.partitionBy(8).cache()
sizes = by_zone.glom().map(len).collect()
print("partitioner:", by_zone.partitioner.numPartitions, "partitions,",
      by_zone.partitioner.partitionFunc.__name__)
print("rows per partition:", sizes)
print("keys in partition 4:", sorted(by_zone.mapPartitionsWithIndex(
    lambda i, it: ({k for k, _ in it} if i == 4 else set())).collect())[:8], "...")

print("\n== 3. mapValues keeps the partitioner, map drops it")
print("mapValues(...).partitioner:", by_zone.mapValues(lambda v: v * 2).partitioner is not None)
print("map(...).partitioner:      ", by_zone.map(lambda kv: (kv[0], kv[1] * 2)).partitioner is not None)

print("\n== 4. a join of two RDDs with the same partitioner")
tips8 = tips.partitionBy(8).cache()
joined = by_zone.reduceByKey(add).join(tips8.reduceByKey(add))
print("result partitions:", joined.getNumPartitions(), " sample:",
      sorted((k, round(f), round(t)) for k, (f, t) in joined.take(200) if k in (132, 138)))
print(joined.toDebugString().decode().replace(here, ""))

print("\n== 5. a custom partition function: one partition for each airport")
airports = {1: 0, 132: 1, 138: 2}                             # EWR, JFK, LaGuardia; the rest -> 3
custom = fares.partitionBy(4, lambda k: airports.get(k, 3))
print("rows per partition (EWR, JFK, LGA, other):", custom.glom().map(len).collect())
