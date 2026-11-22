"""Chapter 10: nanosecond timestamps, a preview in Spark 4.3.

pandas and PyArrow write timestamp[ns] to Parquet by default. This script writes such a file, then reads it
with the preview flag off and on, and shows what works in SQL and what PySpark can and cannot bring back.
Run from code/:  uv run python ch10/nanos.py
"""
import re
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa
import pyarrow.parquet as pq
from common.book import session

spark = session("ch10-nanos", spark_sql_session_timeZone="UTC")
tmp = Path(tempfile.mkdtemp(prefix="ch10-nanos-"))


def attempt(f, width=100):
    try:
        return f()
    except Exception as e:
        for line in str(e).splitlines():
            m = re.search(r"\[[A-Z_.]+\].*", line)
            if m:
                return "ERROR " + m.group(0)[:width]
        return "ERROR " + str(e).splitlines()[0][:width]


# Two events one nanosecond apart, as a pandas or PyArrow pipeline would write them.
events = pa.table({
    "event_ts": pa.array([1780308000123456789, 1780308000123456790], type=pa.timestamp("ns")),
    "event_ts_utc": pa.array([1780308000123456789, 1780308000123456790], type=pa.timestamp("ns", tz="UTC")),
    "id": [1, 2]})
pq.write_table(events, tmp / "events.parquet")
print("== the file")
for f in pq.ParquetFile(tmp / "events.parquet").schema:
    print(f"  {f.name:<13} {f.physical_type} {f.logical_type}")

print("\n== 1. preview flag off (the default)")
print("spark.sql.timestampNanosTypes.enabled =", spark.conf.get("spark.sql.timestampNanosTypes.enabled"))
print("read:", attempt(lambda: spark.read.parquet(str(tmp)).columns))
print("CAST to TIMESTAMP_NTZ(9):", attempt(lambda: spark.sql(
    "SELECT CAST('2026-06-01 10:00:00.123456789' AS TIMESTAMP_NTZ(9))").collect(), 120))

print("\n== 2. preview flag on")
spark.conf.set("spark.sql.timestampNanosTypes.enabled", "true")
df = spark.read.parquet(str(tmp))
print(df.selectExpr("typeof(event_ts)", "typeof(event_ts_utc)").first())
df.selectExpr("id", "cast(event_ts AS string) AS event_ts",
              "date_format(event_ts, 'HH:mm:ss.SSSSSSSSS') AS fraction",
              "unix_micros(event_ts) AS micros").show(truncate=False)
print(df.selectExpr("count(DISTINCT event_ts) AS nanos",
                    "count(DISTINCT CAST(event_ts AS TIMESTAMP_NTZ)) AS micros").first())
print(df.selectExpr("max(event_ts) > min(event_ts) AS later", "max(event_ts) - min(event_ts) AS diff").first())
print(spark.sql("SELECT typeof(timestamp_nanos(1780308000123456789)) AS t, "
                "cast(timestamp_nanos(1780308000123456789) AS string) AS v").first())
print(spark.sql("SELECT typeof(TIMESTAMP_NTZ'2026-06-01 10:00:00.123456789') AS literal_type").first())

print("\n== 3. what PySpark (Classic) can bring back")
for label, f in [("df.schema", lambda: df.schema), ("collect()", lambda: df.collect()),
                 ("toPandas()", lambda: df.toPandas()), ("toArrow()", lambda: df.toArrow()),
                 ("cast to string, collect()", lambda: df.selectExpr("cast(event_ts AS string) AS s").collect()),
                 ("cast to micros, collect()", lambda: df.selectExpr("cast(event_ts AS timestamp_ntz) AS t").collect())]:
    print(f"  {label:<26} {str(attempt(f, 70))[:96]}")

print("\n== 4. writing it back keeps nanoseconds")
df.write.parquet(str(tmp / "out"))
part = next((tmp / "out").glob("*.parquet"))
for f in pq.ParquetFile(part).schema:
    print(f"  {f.name:<13} {f.physical_type} {f.logical_type}")
print("  values:", pq.read_table(part).column("event_ts").to_pylist())
print("time_bucket on nanos:", attempt(lambda: spark.sql(
    "SELECT time_bucket(INTERVAL 1 SECOND, CAST('2026-06-01 10:00:00.123456789' AS TIMESTAMP_NTZ(9)))").collect(), 60))
