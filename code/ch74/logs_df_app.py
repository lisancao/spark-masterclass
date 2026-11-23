"""Chapter 74, Step 11: an application whose JVM log is JSON (spark.log.structuredLogging.enabled=true).
It runs one shuffle at INFO level, and one task that fails once and then succeeds on a retry.
"""
from pyspark import TaskContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch74-json-logs").getOrCreate()
spark.sparkContext.setLogLevel("INFO")


def flaky(it):
    tc = TaskContext.get()
    if tc.partitionId() == 1 and tc.attemptNumber() == 0:
        raise ValueError("first attempt of partition 1 fails on purpose")
    yield sum(1 for _ in it)


print(spark.range(0, 100_000, numPartitions=4).selectExpr("id % 7 AS k").groupBy("k").count().count())
print(sum(spark.sparkContext.parallelize(range(1000), 4).mapPartitions(flaky).collect()))
spark.stop()
