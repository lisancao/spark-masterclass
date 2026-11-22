"""Chapter 5: Spark's types, how a schema is written down, and what nullability promises.

Run from code/:  uv run python ch05/types.py
"""
import json
import os
import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Row, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, ArrayType
from common.book import session

HERE = Path(__file__).resolve().parent
EVENTS = HERE.parent / "data" / "events"
spark = session("ch05-types", spark_sql_session_timeZone="UTC")


def err(f):
    try:
        r = f()
        return r
    except Exception as e:
        return "ERROR " + str(e).splitlines()[0][:96]


# 1. Literal types.
print("== 1. typeof over literals")
lits = ["true", "1Y", "1S", "1", "1L", "1.5F", "1.5D", "1.5", "'x'", "X'00'", "DATE'2026-06-01'",
        "TIMESTAMP'2026-06-01 10:00:00'", "TIMESTAMP_NTZ'2026-06-01 10:00:00'", "INTERVAL 2 DAYS",
        "INTERVAL '1-2' YEAR TO MONTH", "array(1, 2)", "map('a', 1)", "named_struct('id', 1, 'plan', 'pro')",
        "parse_json('{\"a\": 1}')", "'x' COLLATE UTF8_LCASE", "TIME'10:07:00'"]
for lit, t in zip(lits, spark.sql("SELECT " + ", ".join(f"typeof({l})" for l in lits)).first()):
    print(f"  {lit:<40} {t}")

# 2. Numbers: overflow is an error under ANSI, doubles are binary fractions, decimals are exact.
print("\n== 2. numbers")
for q in ["SELECT CAST(300 AS TINYINT)", "SELECT 2147483647 + 1",
          "SELECT CAST(12345.678 AS DECIMAL(5,2))"]:
    print(f"  {q:<44}", err(lambda: spark.sql(q).first()[0]))
print("  ", spark.sql("SELECT 0.1D + 0.2D AS dbl, 0.1BD + 0.2BD AS dec, "
                      "CAST(123.456 AS DECIMAL(5,2)) AS rounded, "
                      "try_cast(12345.678 AS DECIMAL(5,2)) AS too_big").first())
from pyspark.sql.types import DecimalType
print("   DecimalType() is", DecimalType().simpleString())

# 3. Two timestamp types: an instant and a wall-clock reading.
print("\n== 3. TIMESTAMP vs TIMESTAMP_NTZ under two session time zones")
q = ("SELECT TIMESTAMP '2026-06-01 10:00:00' AS ts, TIMESTAMP_NTZ '2026-06-01 10:00:00' AS ntz, "
     "unix_timestamp(TIMESTAMP '2026-06-01 10:00:00') AS ts_epoch, "
     "unix_seconds(TIMESTAMP_NTZ '2026-06-01 10:00:00') AS ntz_epoch")
for tz in ["UTC", "America/New_York"]:
    spark.conf.set("spark.sql.session.timeZone", tz)
    r = spark.sql(q).first()
    print(f"  {tz:<17} ts_epoch={r.ts_epoch}  ntz_epoch={r.ntz_epoch}")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# collect() turns a TIMESTAMP into a naive datetime in the *Python process's* zone.
probe = ("import sys; sys.path.insert(0, '.'); from common.book import session; "
         "s = session('tz', spark_sql_session_timeZone='UTC'); "
         "r = s.sql(\"SELECT TIMESTAMP '2026-06-01 10:00:00' AS ts\"); "
         "print(repr(r.first().ts), '| toPandas:', r.toPandas().ts[0])")
for tz in ["UTC", "Asia/Ho_Chi_Minh"]:
    out = subprocess.run([sys.executable, "-c", probe], cwd=HERE.parent, capture_output=True, text=True,
                         env={**os.environ, "TZ": tz, "SPARK_UI_PORT": "4053"}).stdout.strip()
    print(f"  python TZ={tz:<17} collect: {out}")

print("  interval day to second ->", repr(spark.sql(
    "SELECT TIMESTAMP '2026-06-03 12:00:00' - TIMESTAMP '2026-06-01 10:00:00' AS d").first().d))
print("  interval year to month ->", err(lambda: repr(spark.sql(
    "SELECT INTERVAL '1-2' YEAR TO MONTH AS ym").first().ym)))
print("  TIME literal           ->", err(lambda: repr(spark.sql("SELECT TIME'10:07:00' AS t").first().t)))
print("  CAST('10:07' AS TIME)  ->", err(lambda: spark.sql("SELECT CAST('10:07:00' AS TIME)").first()))

# 4. Writing a schema down: DDL and StructType, and the ways to print one.
print("\n== 4. DDL, StructType, JSON")
ddl = ("id BIGINT NOT NULL, name STRING, tags ARRAY<STRING>, "
       "address STRUCT<city: STRING, zip: STRING>, amount DOUBLE COMMENT 'order total in CAD'")
s = StructType.fromDDL(ddl)
built = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("address", StructType([StructField("city", StringType()), StructField("zip", StringType())])),
])
print(s.simpleString())
print(s.toDDL())
print(s.treeString())
print("comment lives in metadata:", s["amount"].metadata)
print("fromDDL == hand-built (without amount)?", StructType(s.fields[:4]) == built)
text = s.json()
print(text[:88])
print("round trip equal:", StructType.fromJson(json.loads(text)) == s)
loose = StructType([StructField(f.name, f.dataType, True) for f in built.fields])
print("differ only in nullability: ==", loose == built, "| simpleString equal",
      loose.simpleString() == built.simpleString())

# 5. Nested types: paths in columns, paths in schema objects.
print("\n== 5. nested types and paths")
events = spark.read.json(str(EVENTS))
print(events.schema["user"].dataType["geo"])
print(events.schema.fieldNames())
nested = spark.sql("SELECT named_struct('geo', named_struct('country', 'CA')) AS user, "
                   "array(10, 20) AS items, map('color', 'red') AS tags")
nested.select(F.col("user.geo.country"), F.col("items")[0], F.col("tags")["color"]).show()

# 6. Nullability: file readers drop the promise, createDataFrame keeps it.
print("== 6. nullability")
d = spark.read.schema("event STRING NOT NULL, user_id BIGINT NOT NULL").json(str(EVENTS))
print(d.schema.simpleString(), [f.nullable for f in d.schema.fields])
print("events with no user_id:", d.where("user_id IS NULL").count(), "of", d.count())
st = StructType([StructField("id", IntegerType(), False)])
print("createDataFrame with None:", err(lambda: spark.createDataFrame([(1,), (None,)], st).collect()))
print("verifySchema=False:", spark.createDataFrame([(1,), (None,)], st, verifySchema=False).collect())

# 7. From Python data to a DataFrame and back.
print("\n== 7. createDataFrame and toArrow")
import pandas as pd
import pyarrow as pa
rows = [Row(zone="JFK Airport", trips=142209), Row(zone="Midtown Center", trips=156493)]
print(spark.createDataFrame(rows).schema.simpleString())
print(spark.createDataFrame(pd.DataFrame({"zone": ["JFK"], "fare": [62.27]})).schema.simpleString())
print(spark.createDataFrame(pa.table({"zone": ["JFK"], "fare": [62.27]})).schema.simpleString())
print(type(spark.range(3).toArrow()).__name__, spark.range(3).toArrow().schema)
print("arrow for pandas conversions:", spark.conf.get("spark.sql.execution.arrow.pyspark.enabled"))
