"""Chapter 47, Step 3: an application that always fails after one job."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("flaky").getOrCreate()
print("rows:", spark.range(1000).count())
raise RuntimeError("the input folder is empty")
