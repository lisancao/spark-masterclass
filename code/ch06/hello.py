"""Chapter 6: the smallest useful Spark program. Works with any install that has a driver."""
import platform
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hello").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
path = sys.argv[1] if len(sys.argv) > 1 else "/data/nyc-taxi/yellow"   # Docker mounts the data at /data
trips = spark.read.parquet(path)
print(f"python {platform.python_version()}, spark {spark.version}, {trips.count():,} trips")
