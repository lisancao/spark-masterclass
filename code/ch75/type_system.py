"""Chapter 75: the SQL type system in 4.3.0. Each DDL type name, what Spark makes of it,
and the PySpark class it becomes, tried in a CAST and in a CREATE TABLE.

Run from code/:  uv run python ch75/type_system.py > ch75/output/type_system.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work

W = work("ch75-types")
spark = session("ch75-types", **{"spark.sql.warehouse.dir": str(W / "warehouse")})

TYPES = [
    "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL(10,2)",
    "STRING", "STRING COLLATE UTF8_LCASE", "CHAR(3)", "VARCHAR(10)", "BINARY",
    "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "TIME(6)", "TIMESTAMP_NTZ(9)",
    "INTERVAL DAY TO SECOND", "INTERVAL YEAR TO MONTH",
    "ARRAY<INT>", "MAP<STRING,INT>", "STRUCT<a:INT,b:STRING>",
    "VARIANT", "GEOMETRY(4326)", "GEOGRAPHY(4326)",
]


def short(e):
    m = __import__("re").search(r"\[([A-Z_.]+)\]", str(e))
    return m.group(1) if m else type(e).__name__


print("== 1. CAST(NULL AS <type>): the SQL type name and the PySpark class")
print(f"  {'DDL':<27} {'typeof()':<27} {'PySpark class'}")
for t in TYPES:
    try:
        name = spark.sql(f"SELECT typeof(CAST(NULL AS {t})) AS t").first().t
        cls = type(spark.sql(f"SELECT CAST(NULL AS {t}) AS v").schema["v"].dataType).__name__
        print(f"  {t:<27} {name:<27} {cls}")
    except Exception as e:
        print(f"  {t:<27} error: {short(e)}")

print("\n== 2. CHAR and VARCHAR in a table keep their length and check it")
spark.sql("CREATE TABLE codes (code CHAR(3), label VARCHAR(5)) USING parquet")
spark.sql("DESCRIBE codes").show(truncate=False)
spark.sql("INSERT INTO codes VALUES ('A', 'short')")
print("  read back:", [(r.code, len(r.code)) for r in spark.table("codes").collect()],
      "(CHAR pads to its length)")
try:
    spark.sql("INSERT INTO codes VALUES ('ABCD', 'x')")
except Exception as e:
    print("  insert 'ABCD' into CHAR(3):", short(e))

print("\n== 3. TIME and the nanosecond types sit behind internal flags in 4.3.0")
print("  TIME literal, flags off:      TIME'10:07:00' ->",
      spark.sql("SELECT typeof(TIME'10:07:00')").first()[0])
for flag, t in [("spark.sql.timeType.enabled", "TIME(6)"),
                ("spark.sql.timestampNanosTypes.enabled", "TIMESTAMP_NTZ(9)")]:
    print(f"  {flag:<38} = {spark.conf.get(flag)}")
    spark.conf.set(flag, "true")
    r = spark.sql(f"SELECT typeof(CAST(NULL AS {t})) AS t").first()
    print(f"  after SET {flag}=true: CAST(NULL AS {t}) -> {r.t}")
spark.stop()
