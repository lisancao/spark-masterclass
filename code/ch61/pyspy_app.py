"""Chapter 61, Step 12: a slow UDF for py-spy to sample. Run by pyspy.sh."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("pyspy").getOrCreate()


@F.udf("long")
def slow(x):
    h = 0
    for i in range(2000):
        h = (h * 31 + x + i) % 1000003
    return h


print(spark.range(0, 40_000, numPartitions=2).select(F.sum(slow("id"))).first()[0])
spark.stop()
