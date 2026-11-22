"""Chapter 16, Step 1: what geospatial support Spark 4.3.0 has. Functions in SQL, PySpark and Scala,
the types, the flag, and the names that other engines have but Spark 4.3.0 does not.

Run from code/:  uv run python ch16/inventory.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyspark
from pyspark.sql import functions as F
from geo76 import attempt
from common.book import session

spark = session("ch16-inventory")

print("== 1. ST functions in the function registry")
print("Spark", spark.version, "| pyspark", pyspark.__version__)
names = [r.function for r in spark.sql("SHOW FUNCTIONS LIKE 'st_*'").collect()]
print("SQL     :", names)
print("PySpark :", sorted(n for n in dir(F) if n.startswith("st_")))
jf = spark.sparkContext._jvm.java.lang.Class.forName("org.apache.spark.sql.functions")
scala = sorted({m.getName() for m in jf.getMethods() if m.getName().startswith("st_")})
print("Scala   :", scala)
total = spark.sql("SHOW FUNCTIONS").count()
print(f"all built-in and session functions: {total}")

print("\n== 2. signatures (DESCRIBE FUNCTION)")
for n in names:
    usage = [r[0] for r in spark.sql(f"DESCRIBE FUNCTION {n}").collect() if r[0].startswith("Usage")]
    for line in usage[0].replace("Usage:", "").strip().split("\n"):
        if line.strip():
            print(" ", " ".join(line.split())[:110])

print("\n== 3. the types (DDL strings)")
for ddl in ["GEOMETRY(4326)", "GEOMETRY(2263)", "GEOMETRY(0)", "GEOMETRY(ANY)",
            "GEOGRAPHY(4326)", "GEOGRAPHY(ANY)", "GEOGRAPHY(3857)", "GEOMETRY"]:
    attempt(ddl, lambda d=ddl: spark.sql(f"SELECT CAST(NULL AS {d}) AS g").schema[0].dataType, 18)

print("\n== 4. the flag spark.sql.geospatial.enabled")
print("  default:", spark.conf.get("spark.sql.geospatial.enabled"))
spark.conf.set("spark.sql.geospatial.enabled", "false")
attempt("st_srid with the flag off",
        lambda: spark.sql("SELECT st_srid(st_geogfromwkb(X'0101000000000000000000F03F0000000000000040'))").first(), 26)
attempt("CAST AS GEOMETRY(0), flag off", lambda: spark.sql("SELECT CAST(NULL AS GEOMETRY(0))").schema, 26)
spark.conf.unset("spark.sql.geospatial.enabled")

print("\n== 5. names from PostGIS and Sedona that 4.3.0 does not have")
for fn in ["st_point(1, 2)", "st_geomfromtext('POINT (1 2)')", "st_geomfromgeojson('{}')",
           "st_astext(g)", "st_x(g)", "st_contains(g, g)", "st_intersects(g, g)",
           "st_distance(g, g)", "st_area(g)", "st_transform(g, 4326)", "h3_longlatash3(1.0, 2.0, 7)"]:
    attempt(fn, lambda f=fn: spark.sql(
        f"SELECT {f} FROM (SELECT st_geomfromwkb(X'0101000000000000000000F03F0000000000000040') g)").first(), 30)
