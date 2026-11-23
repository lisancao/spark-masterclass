"""Chapter 90: what the RDD API does over Spark Connect, and what replaces it.

Start the server first (ch90/connect_server.sh start), then run from code/:
  SPARK_REMOTE=sc://localhost:15117 uv run python ch90/connect.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark import StorageLevel
from pyspark.sql import Observation, functions as F
from common.book import session, TRIPS, ZONES

spark = session("ch90-connect")
print("session:", type(spark).__module__, " server:", spark.version)
trips = spark.read.parquet(str(TRIPS)).select("PULocationID", "fare_amount")


def attempt(label, fn):
    try:
        out = fn()
        print(f"  {label:34} ok: {out}")
    except Exception as e:
        cond = e.getCondition() if hasattr(e, "getCondition") else None
        print(f"  {label:34} {type(e).__name__}: {cond}")
        print(f"  {'':34} {str(e).splitlines()[0][len(cond or '') + 3:][:58]}")


print("== 1. the RDD entry points")
attempt("spark.sparkContext", lambda: spark.sparkContext)
attempt("trips.rdd", lambda: trips.rdd)
attempt("trips.foreachPartition(f)", lambda: trips.limit(1).foreachPartition(lambda it: None))
attempt("trips.mapInPandas(f, schema)", lambda: trips.limit(2).mapInPandas(lambda it: it, trips.schema).count())

print("\n== 2. the DataFrame equivalents run over Connect")
zones = spark.read.option("header", True).csv(str(ZONES))
attempt("join(F.broadcast(zones))", lambda: trips.join(F.broadcast(zones), trips.PULocationID == zones.LocationID).count())
obs = Observation("q")
attempt("observe() + Observation", lambda: (trips.observe(obs, F.count_if(F.col("fare_amount") <= 0).alias("bad"))
                                             .count(), obs.get)[1])
attempt("persist(StorageLevel.DISK_ONLY)", lambda: trips.persist(StorageLevel.DISK_ONLY).storageLevel)
attempt("localCheckpoint()", lambda: trips.groupBy("PULocationID").count().localCheckpoint().count())
attempt("repartition(8, col) partitions", lambda: trips.repartition(8, "PULocationID")
        .select(F.spark_partition_id().alias("p")).distinct().count())
attempt("zipWithIndex() (DataFrame, 4.2+)", lambda: trips.limit(3).zipWithIndex("i").select("i").collect())
