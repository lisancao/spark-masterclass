"""Chapter 74, Step 11: read a JSON Spark log with SPARK_LOG_SCHEMA and ask it questions."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session  # noqa: E402

from pyspark.logger import SPARK_LOG_SCHEMA  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = session("ch74-read-logs", spark_sql_session_timeZone="UTC")   # the log writes UTC (Z)
print(f"SPARK_LOG_SCHEMA = {SPARK_LOG_SCHEMA}")
logs = spark.read.schema(SPARK_LOG_SCHEMA).json(sys.argv[1])
print(f"rows {logs.count()}, rows that are not JSON (all fields null) {logs.where('ts IS NULL').count()}")

print("\nlevels and the loggers with the most lines:")
logs.where("ts IS NOT NULL").groupBy("level").count().orderBy(F.desc("count")).show()
logs.groupBy("logger").count().orderBy(F.desc("count")).limit(5).show(truncate=False)

print("MDC keys in context, and how many lines carry each:")
(logs.select(F.explode(F.map_keys("context")).alias("key")).groupBy("key").count()
     .orderBy(F.desc("count")).limit(8).show(truncate=False))

print("the failed task, with its MDC context and the exception struct:")
(logs.where("level IN ('WARN', 'ERROR') AND exception IS NOT NULL")
     .select(F.date_format("ts", "HH:mm:ss.SSS").alias("ts"), "level", "logger",
             F.col("context")["task_name"].alias("task_name"),
             F.col("exception.class").alias("exception_class"),
             F.size("exception.stacktrace").alias("frames"))
     .show(3, truncate=40))
spark.stop()
