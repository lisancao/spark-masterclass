"""Chapter 81: a small application for the security scripts. It runs one shuffle and prints one line.

    spark-submit probe.py NAME [--hold SECONDS]
"""
import sys
import time

from pyspark.sql import SparkSession

name = sys.argv[1]
spark = SparkSession.builder.appName(name).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
n = spark.range(0, 1_000_000, numPartitions=4).selectExpr("id % 10 AS k").groupBy("k").count().count()
print(f"{name}: ok, {n} groups, executors {spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1}")
if "--hold" in sys.argv:
    time.sleep(int(sys.argv[sys.argv.index("--hold") + 1]))
spark.stop()
