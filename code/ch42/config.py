"""Chapter 42: which settings you can change in a running session, and which ones you cannot.

Run from code/:  uv run python ch42/config.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session

spark = session("ch42-config", spark_driver_memory="4g", spark_ui_showConsoleProgress="false")

KEYS = ["spark.sql.shuffle.partitions", "spark.sql.files.maxPartitionBytes", "spark.sql.adaptive.enabled",
        "spark.sql.autoBroadcastJoinThreshold", "spark.sql.execution.arrow.maxRecordsPerBatch",
        "spark.executor.memory", "spark.driver.memory", "spark.memory.fraction", "spark.default.parallelism",
        "spark.serializer", "spark.python.worker.reuse"]
print(f"{'key':<46}{'value':>12}  isModifiable")
for k in KEYS:
    try:
        v = spark.conf.get(k)
    except Exception:
        v = "(not set)"
    print(f"{k:<46}{v:>12}  {spark.conf.isModifiable(k)}")

print()
spark.conf.set("spark.sql.shuffle.partitions", "64")
print("set spark.sql.shuffle.partitions=64 ->", spark.conf.get("spark.sql.shuffle.partitions"))
for k, v in (("spark.memory.fraction", "0.8"), ("spark.executor.memory", "8g")):
    try:
        spark.conf.set(k, v)
        print(f"set {k}={v} -> accepted")
    except Exception as e:
        print(f"set {k}={v} ->", str(e).splitlines()[0][:120])

# The settings a session started with: the Environment tab lists them, and so does SparkConf.
print("\nspark.sparkContext.getConf() entries that the book's session sets:")
for k, v in sorted(spark.sparkContext.getConf().getAll()):
    if k in ("spark.app.name", "spark.master", "spark.driver.memory", "spark.ui.port",
             "spark.ui.showConsoleProgress"):
        print(f"  {k} = {v}")
