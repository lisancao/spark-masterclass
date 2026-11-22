"""Chapter 34: the Arrow batches a Python worker receives, and what Python values become in Spark.

Classic. Run from code/:  uv run python ch34/arrow_batches.py
"""
import os, subprocess, sys, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import functions as F
from common.book import session, TRIPS

if len(sys.argv) > 1 and sys.argv[1] == "tz":          # child: one worker time zone
    spark = session("ch34-tz", spark_sql_session_timeZone="UTC")
    from datetime import datetime
    naive = F.udf(lambda _: datetime(2026, 6, 1, 8, 0), "timestamp")
    ntz = F.udf(lambda _: datetime(2026, 6, 1, 8, 0), "timestamp_ntz")
    r = spark.range(1).select(naive("id").cast("string").alias("ts"), ntz("id").cast("string").alias("ntz")).first()
    print(f"{os.environ['TZ']:<22}{r.ts:<24}{r.ntz}")
    sys.exit(0)

warnings.filterwarnings("ignore")
spark = session("ch34-arrow", spark_sql_session_timeZone="UTC", spark_ui_showConsoleProgress="false")
trips = spark.read.parquet(str(TRIPS)).select("fare_amount", "trip_distance")


@F.pandas_udf("long")
def batch_rows(fare: pd.Series) -> pd.Series:
    return pd.Series([len(fare)] * len(fare))          # every row carries the size of its batch


def batches(df):
    sizes = df.select(batch_rows("fare_amount").alias("n")).groupBy("n").count().collect()
    return sorted(((r.n, r["count"] // r.n) for r in sizes), reverse=True)


print("Arrow batches into a pandas UDF, June trips (fare_amount only):")
CI = "spark.sql.execution.arrow.pythonUDF.columnarInput.enabled"
MR = "spark.sql.execution.arrow.maxRecordsPerBatch"
CASES = [{}, {MR: "50000"}, {"spark.sql.parquet.columnarReaderBatchSize": "20000"},
         {CI: "false"}, {CI: "false", MR: "50000"},
         {CI: "false", MR: "0", "spark.sql.execution.arrow.maxBytesPerBatch": "1048576"}]
for conf in CASES:
    for k, v in conf.items():
        spark.conf.set(k, v)
    b = batches(trips)
    label = ", ".join(f"{k.split('.')[-1] if k != CI else 'columnarInput'}={v}" for k, v in conf.items()) or "defaults"
    label = label.replace("maxRecordsPerBatch", "maxRecords").replace("maxBytesPerBatch", "maxBytes")
    print(f"  {label:<52}{sum(n for _, n in b):>5} batches; sizes: "
          + ", ".join(f"{s:,} x{n}" for s, n in b[:2]))
    for k in conf:
        spark.conf.unset(k)
spark.conf.set(MR, "-1")
try:
    batches(trips)
    print("  maxRecordsPerBatch=-1: ran")
except Exception as e:
    import re
    m = re.search(r"java\.lang\.\w+Exception: \S+", str(e))
    print("  maxRecordsPerBatch=-1:", m.group(0) if m else str(e)[:80])
spark.conf.unset(MR)
print("defaults:", {(k.split(".")[-1] if k != CI else "columnarInput.enabled"): spark.conf.get(k) for k in (MR, "spark.sql.execution.arrow.maxBytesPerBatch",
                                                               CI, "spark.sql.parquet.columnarReaderBatchSize")})

# What a returned Python value becomes, with Arrow (the default) and with pickled rows.
VALUES = [("int", 7.9), ("int", "7"), ("int", True), ("int", 2 ** 40), ("double", 3), ("string", 42),
          ("boolean", 1), ("date", "2026-06-01")]
print(f"\n{'declared':<10}{'returned':<16}{'Arrow (default)':<34}{'useArrow=False'}")
for dtype, value in VALUES:
    out = []
    for arrow in (True, False):
        f = F.udf(lambda _, v=value: v, dtype, useArrow=arrow)
        try:
            out.append(repr(spark.range(1).select(f("id").alias("x")).first().x))
        except Exception as e:
            import re
            m = re.findall(r"(?:pyarrow\.lib\.)?(\w+(?:Error|Invalid)): ", str(e))
            out.append("error " + (m[-1] if m else type(e).__name__)[:26])
    print(f"{dtype:<10}{value!r:<16}{out[0]:<34}{out[1]}")

# The same 7.9 for an int from a pandas UDF, with convertToArrowArraySafely (default true since 4.1).
@F.pandas_udf("int")
def seven_nine(s: pd.Series) -> pd.Series:
    return pd.Series([7.9] * len(s))


print("\nspark.sql.execution.pandas.convertToArrowArraySafely =",
      spark.conf.get("spark.sql.execution.pandas.convertToArrowArraySafely"))
try:
    print("@pandas_udf returns 7.9 for int ->", spark.range(1).select(seven_nine("id").alias("x")).first().x)
except Exception as e:
    import re
    m = re.findall(r"(\w+(?:Error|Invalid)): ([^\n]{0,60})", str(e))
    print("@pandas_udf returns 7.9 for int -> error", m[-1][0] if m else type(e).__name__, "|", m[-1][1] if m else "")

# A naive datetime from a UDF is read in the worker's time zone, not the session's.
print(f"\nnaive datetime(2026, 6, 1, 8, 0) returned as timestamp and as timestamp_ntz; session zone UTC")
print(f"{'worker TZ':<22}{'timestamp':<24}{'timestamp_ntz'}")
for tz in ("UTC", "America/Los_Angeles", "Asia/Ho_Chi_Minh"):
    env = dict(os.environ, TZ=tz, SPARK_UI_PORT=str(int(os.environ.get("SPARK_UI_PORT", "4040")) + 1))
    r = subprocess.run([sys.executable, __file__, "tz"], env=env, capture_output=True, text=True)
    print(r.stdout.strip() or r.stderr.strip().splitlines()[-1])
