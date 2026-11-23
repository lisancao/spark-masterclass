"""Chapter 22, Steps 2 and 3: the two types, SRIDs, casts, the WKB rules and the Python objects.

Run from code/:  uv run python ch22/geotypes.py
"""
import struct
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F
from pyspark.sql.types import Geometry, Geography, GeometryType, StructType, StructField, LongType
from geo76 import attempt
from common.book import session

spark = session("ch22-types")
PT = "X'0101000000000000000000F03F0000000000000040'"    # POINT (1 2), little-endian


def one(sql):
    return spark.sql(sql).first()[0]


print("== 1. constructors and the SRID of the result")
for expr in [f"st_geomfromwkb({PT})", f"st_geomfromwkb({PT}, 3857)", f"st_geogfromwkb({PT})",
             f"st_setsrid(st_geomfromwkb({PT}), 4326)", f"st_setsrid(st_geogfromwkb({PT}), 4269)"]:
    attempt(expr.replace(PT, "<wkb>"), lambda e=expr: one(f"SELECT typeof({e})"), 42)

print("\n== 2. st_asbinary: the same point, two byte orders")
print("  NDR (default):", one(f"SELECT hex(st_asbinary(st_geomfromwkb({PT})))"))
print("  XDR          :", one(f"SELECT hex(st_asbinary(st_geomfromwkb({PT}), 'XDR'))"))
xdr = bytes.fromhex(one(f"SELECT hex(st_asbinary(st_geomfromwkb({PT}), 'XDR'))"))
print("  XDR read back:", one(f"SELECT hex(st_asbinary(st_geomfromwkb(X'{xdr.hex()}')))"))

print("\n== 3. casts between the types")
for label, sql in [
    ("GEOGRAPHY(4326) -> GEOMETRY(4326)", f"SELECT typeof(CAST(st_geogfromwkb({PT}) AS GEOMETRY(4326)))"),
    ("GEOMETRY(4326) -> GEOGRAPHY(4326)",
     f"SELECT typeof(CAST(st_setsrid(st_geomfromwkb({PT}), 4326) AS GEOGRAPHY(4326)))"),
    ("GEOMETRY(0) -> GEOGRAPHY(4326)", f"SELECT typeof(CAST(st_geomfromwkb({PT}) AS GEOGRAPHY(4326)))"),
    ("GEOMETRY(3857) -> GEOMETRY(ANY)", f"SELECT typeof(CAST(st_geomfromwkb({PT}, 3857) AS GEOMETRY(ANY)))"),
    ("GEOMETRY(3857) -> GEOMETRY(4326)", f"SELECT typeof(CAST(st_geomfromwkb({PT}, 3857) AS GEOMETRY(4326)))"),
    ("GEOMETRY(0) -> BINARY", f"SELECT typeof(CAST(st_geomfromwkb({PT}) AS BINARY))"),
    ("BINARY -> GEOMETRY(0)", f"SELECT typeof(CAST({PT} AS GEOMETRY(0)))"),
    ("GEOMETRY(0) -> STRING", f"SELECT CAST(st_geomfromwkb({PT}) AS STRING)"),
]:
    attempt(label, lambda s=sql: one(s), 34)

print("\n== 4. the WKB rules at parse time")


def ring(*pts):
    return struct.pack("<I", len(pts)) + b"".join(struct.pack("<dd", *p) for p in pts)


def poly(*pts):
    return struct.pack("<BII", 1, 3, 1) + ring(*pts)


nan, inf = float("nan"), float("inf")
cases = [
    ("point (1 2)", struct.pack("<BIdd", 1, 1, 1, 2)),
    ("point (NaN NaN), POINT EMPTY", struct.pack("<BIdd", 1, 1, nan, nan)),
    ("point (inf 2)", struct.pack("<BIdd", 1, 1, inf, 2)),
    ("point (200 95)", struct.pack("<BIdd", 1, 1, 200, 95)),
    ("closed square, 5 points", poly((0, 0), (1, 0), (1, 1), (0, 1), (0, 0))),
    ("open ring", poly((0, 0), (1, 0), (1, 1), (0, 1))),
    ("ring of 3 points", poly((0, 0), (1, 0), (0, 0))),
    ("linestring, 1 point", struct.pack("<BII", 1, 2, 1) + struct.pack("<dd", 0, 0)),
    ("truncated (3 bytes)", b"\x01\x01\x00"),
]
spark.createDataFrame([(n, b) for n, b in cases], "case STRING, wkb BINARY").createOrReplaceTempView("cases")
print(f"  {'case':<30} {'GEOMETRY':<30} GEOGRAPHY")
for name, _ in cases:
    res = []
    for fn in ["st_geomfromwkb", "st_geogfromwkb"]:
        try:
            r = one(f"SELECT length(st_asbinary({fn}(wkb))) FROM cases WHERE case = '{name}'")
            res.append(f"ok, {r} bytes")
        except Exception as e:  # noqa: BLE001
            msg = str(e).split("\n")[0]
            res.append(msg.split("Error parsing WKB: ")[-1].split(" at position")[0][:30])
    print(f"  {name:<30} {res[0]:<30} {res[1]}")

print("\n== 5. values in Python")
g = spark.sql(f"SELECT st_geomfromwkb({PT}, 3857) AS geom, st_geogfromwkb({PT}) AS geog").first()
print("  geom:", type(g.geom).__name__, "srid", g.geom.getSrid(), "bytes", g.geom.getBytes().hex())
print("  geog:", type(g.geog).__name__, "srid", g.geog.getSrid())
schema = StructType([StructField("id", LongType()), StructField("g", GeometryType(4326))])
df = spark.createDataFrame([(1, Geometry.fromWKB(bytes.fromhex(PT[2:-1]), 4326))], schema)
print("  createDataFrame with GeometryType(4326):", df.selectExpr("st_srid(g)").first()[0])
attempt("a value with SRID 0 in a GEOMETRY(4326) column",
        lambda: spark.createDataFrame([(2, Geometry.fromWKB(bytes.fromhex(PT[2:-1]), 0))], schema).collect(), 46)

print("\n== 6. SRIDs that the JVM accepts and PySpark 4.3.0 cannot describe")
for srid in [3857, 2263, 32618, 4269]:
    df = spark.sql(f"SELECT st_geomfromwkb({PT}, {srid}) AS g")
    jvm = one(f"SELECT typeof(st_geomfromwkb({PT}, {srid}))")
    attempt(f"{srid}: JVM type {jvm}, df.schema", lambda d=df: d.schema[0].dataType, 36)
print("  2263 as BINARY, collected:", spark.sql(
    f"SELECT st_srid(g) AS srid, hex(st_asbinary(g)) AS wkb FROM (SELECT st_geomfromwkb({PT}, 2263) g)").first())
print("  JVM registry entries:", spark.sparkContext._jvm.org.apache.spark.sql.internal.types
      .CartesianSpatialReferenceSystemMapper.getStringId(2263))
