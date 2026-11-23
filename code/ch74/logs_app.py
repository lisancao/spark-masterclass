"""Chapter 74, Step 11: an application that logs from the JVM and from Python. Run by logs.sh."""
from pyspark.logger import PySparkLogger
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("logs").getOrCreate()
log = PySparkLogger.getLogger("ch74")
log.warning("python side", rows=spark.range(5).count())
spark.stop()
