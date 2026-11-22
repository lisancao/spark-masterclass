"""Chapter 64, Step 6: the idioms that a version-pinned skill writes down, each checked on 4.3.0.

Run from code/:  uv run python ch64/idioms.py
"""
import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

# 1. The module name: Databricks Lakeflow's `dlt` is not in open source Spark.
for name in ("dlt", "pyspark.pipelines"):
    try:
        importlib.import_module(name)
        print(f"import {name:18} ok")
    except ImportError as e:
        print(f"import {name:18} {type(e).__name__}: {e}")

spark = session("idioms", spark_master="local[1]")

# 2. A day boundary depends on the session time zone, unless the code says UTC.
events = spark.createDataFrame([("2026-06-24T23:30:00Z",), ("2026-06-25T06:30:00Z",)], "event_time string")
for tz in ("UTC", "America/Los_Angeles"):
    spark.conf.set("spark.sql.session.timeZone", tz)
    naive = [str(r[0]) for r in events.select(F.to_date(F.to_timestamp("event_time"))).collect()]
    utc = [str(r[0]) for r in events.select(
        F.to_date(F.to_utc_timestamp(F.to_timestamp("event_time"), F.current_timezone()))).collect()]
    print(f"session time zone {tz:20} to_date(ts): {naive}   UTC day: {utc}")

# 3. A ms epoch read as seconds: no error, a date in the far future.
ms = spark.createDataFrame([(1782345600000,)], "event_ms long")
print("epoch ms cast as seconds:", ms.select(F.col("event_ms").cast("timestamp").cast("string")).first()[0])
print("epoch ms, divided first: ", ms.select(F.timestamp_millis("event_ms").cast("string")).first()[0])
spark.stop()
