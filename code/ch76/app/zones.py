"""Chapter 76: a PySpark application that the operator runs as a SparkApplication.
It generates trips in the executors, scores them with a default (Arrow) UDF, and prints a summary."""
import os
import socket
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("zones").getOrCreate()


@F.udf("string")
def where(_):
    return socket.gethostname()


trips = (spark.range(0, 2_000_000, numPartitions=8)
         .withColumn("zone", (F.col("id") * 7919 % 263).cast("int"))
         .withColumn("fare", (F.col("id") % 50 + 2.5)))
top = trips.groupBy("zone").agg(F.count("*").alias("trips"), F.round(F.avg("fare"), 2).alias("avg_fare"))
print("top zones:", [tuple(r) for r in top.orderBy(F.desc("trips"), "zone").limit(3).collect()])
pods = trips.select(where("id").alias("pod")).groupBy("pod").count().orderBy("pod").collect()
print("rows by executor pod:", [(r.pod, r["count"]) for r in pods])
print("driver pod:", socket.gethostname(), " spark", spark.version, " python", os.sys.version.split()[0])
spark.stop()
