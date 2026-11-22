"""Chapter 24: the columnar machinery that plain Spark 4.3.0 already has, with no engine loaded.

Prints which operators of a query support columnar output, where Spark put a transition,
and the configs an engine uses to plug in (all unset here).
Run from code/:  uv run python ch24/columnar_43.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from ch24.engines import start, queries, tables, walk, kind, OFFLOAD_FRIENDLY

# A static conf, so it is set at start: the Arrow cache serializer that is new in 4.3.
spark = start("cpu", **OFFLOAD_FRIENDLY, **{
    "spark.sql.cache.serializer":
        "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer"})
print(f"Spark {spark.version}, no engine\n")
for key in ["spark.plugins", "spark.sql.extensions", "spark.shuffle.manager",
            "spark.memory.offHeap.enabled"]:
    print(f"  {key:<30} = {spark.conf.get(key, '(not set)')}")


def show(title, df):
    df.collect()
    print(f"\n{title}\n  {'operator':<40} supportsColumnar")
    for depth, op, columnar in walk(df):
        if kind(op) != "glue":
            print(f"  {'  ' * depth + op:<40} {columnar}")


hourly = queries(spark)["hourly"]
show("hourly, from Parquet:", hourly)

trips, _ = tables(spark)
cached = trips.select("PULocationID", "fare_amount").cache()
cached.count()                                        # fill the cache
by_zone = cached.groupBy("PULocationID").agg(F.sum("fare_amount").alias("fares"))
show("an aggregate over a cached DataFrame (ArrowCachedBatchSerializer):", by_zone)
