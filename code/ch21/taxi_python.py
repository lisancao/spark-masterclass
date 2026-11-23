"""The reference query for Chapter 21, in PySpark over Connect: the five busiest pickup zones in June 2026."""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.book import TRIPS  # noqa: E402
import pyspark  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15106")
t0 = time.perf_counter()
with SparkSession.builder.remote(remote).getOrCreate() as spark:
    print(f"pyspark {pyspark.__version__} -> server {spark.version} at {remote}")
    top = (spark.read.parquet(str(TRIPS))
           .where((F.col("tpep_pickup_datetime") >= "2026-06-01") &
                  (F.col("tpep_pickup_datetime") < "2026-07-01"))
           .groupBy("PULocationID")
           .agg(F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
           .orderBy(F.desc("trips"))
           .limit(5))
    top.show()
print(f"wall time {time.perf_counter() - t0:.2f} s")
