"""Chapter 75, Step 7: where print() and logging in a UDF go, and the 4.1 worker log table.

Run from code/:  uv run python ch75/worker_logs.py 2> worker_stderr.txt
"""
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402
from pyspark.logger import PySparkLogger  # noqa: E402
from common.book import session  # noqa: E402

spark = session("ch75-logs", spark_ui_showConsoleProgress="false")
mode = "Connect" if os.environ.get("SPARK_REMOTE") else "Classic"


@F.udf("string")
def check_fare(fare):
    import logging
    if fare < 0:
        print(f"print: negative fare {fare}")
        logging.getLogger("fares").warning("negative fare %s", fare)
    return "ok" if fare >= 0 else "bad"


def query():
    return spark.createDataFrame([(12.5,), (-8.0,)], "fare double").select(check_fare("fare").alias("c"))


df = query()
print(f"== {mode}, worker logging off (default): result {[r.c for r in df.collect()]}")
print("   print() and logging went to the worker's stderr (see worker_stderr.txt)")

spark.conf.set("spark.sql.pyspark.worker.logging.enabled", "true")
print(f"\n== spark.sql.pyspark.worker.logging.enabled=true")
df.collect()
print(f"   the same DataFrame object again: {spark.tvf.python_worker_logs().count()} log rows")
query().collect()
logs = spark.tvf.python_worker_logs()
print(f"   a new DataFrame:                 {logs.count()} log rows")
print(f"   spark.tvf.python_worker_logs() columns: {logs.columns}")
(logs.select("level", "logger", "msg", F.col("context").cast("string").alias("context"))
     .show(truncate=60))

print("== PySparkLogger on the driver: JSON lines with the context you add")
log = PySparkLogger.getLogger("ch75")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(log.handlers[0].formatter if log.handlers else logging.Formatter())
log.handlers = [handler]
log.warning("zone summary written", zone_count=128, path="/data/zones")
try:
    spark.sql("SELECT 1/0").collect()
except Exception:
    log.exception("query failed", query="SELECT 1/0")
spark.stop()
