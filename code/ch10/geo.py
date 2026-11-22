"""Chapter 10, part 2: GEOMETRY and GEOGRAPHY. WKB bytes, SRIDs, validation, distance, nearest store,
Parquet storage and the limits of the 4.3.0 types.

Data: SparkTutor's courier deliveries (code/data/geo_deliveries): 166 deliveries, 12 stores.
Run from code/:  uv run python ch10/geo.py
"""
import math
import re
import struct
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow.parquet as pq
from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import functions as F, Window
from common.book import session, plan

HERE = Path(__file__).resolve().parents[1] / "data" / "geo_deliveries"
TMP = Path(tempfile.mkdtemp(prefix="ch10-geo-"))
spark = session("ch10-geo", spark_sql_session_timeZone="UTC")
raw = spark.read.option("header", True).csv(str(HERE / "deliveries.csv"))
stores = spark.read.option("header", True).option("inferSchema", True).csv(str(HERE / "stores.csv"))
EARTH_RADIUS_KM = 6371.0
SERVICE_AREAS = [("Canada", 41.0, 84.0, -142.0, -52.0), ("Vietnam", 8.0, 24.0, 102.0, 110.0)]


def err(e):
    if isinstance(e, PySparkException):
        first = next(l for l in str(e).splitlines() if l.strip())
        return f"{e.getCondition()}: " + first.split("] ", 1)[-1][:66]
    cause = re.findall(r"Caused by: ([\w.]+)", str(e))
    if cause:
        return f"{type(e).__name__}, task failed, caused by {cause[-1]}"
    m = re.search(r"(\w+(?:Exception|Error))[:\s]+(\[[A-Z_.0-9]+\])?\s*([^\n]*)", str(e))
    return f"{type(e).__name__} ({m.group(1)}) {m.group(2) or ''}{m.group(3)[:56]}"


def attempt(label, fn):
    try:
        print(f"  {label:<34} {fn()}")
    except (PySparkException, Py4JJavaError) as e:
        print(f"  {label:<34} {err(e)}")


# 1. A point as WKB bytes.
print("== 1. a point in WKB")
wkb = struct.pack("<BIdd", 1, 1, -123.1090, 49.2840)          # Gastown, Vancouver: x = lon, y = lat
print("bytes:", len(wkb), wkb.hex(" ", 1))
print("      order=01 (little-endian) | type=01000000 (Point) | x: 8 bytes | y: 8 bytes")
g = spark.createDataFrame([(wkb,)], "wkb BINARY").select(
    F.st_geogfromwkb("wkb").alias("geog"), F.st_geomfromwkb("wkb").alias("geom"))
print("schema:", [(f.name, f.dataType.simpleString()) for f in g.schema.fields])
print(g.select(F.st_srid("geog").alias("geog_srid"), F.st_srid("geom").alias("geom_srid"),
               F.hex(F.st_asbinary("geog")).alias("back_to_wkb")).first())
print("Python value:", g.first().geog)

# 2. The SRID is a part of the type.
print("\n== 2. SRIDs")
attempt("st_setsrid(geom, 3857)", lambda: g.select(F.st_setsrid("geom", 3857).alias("g")).schema[0].dataType)
attempt("st_setsrid(geog, 3857)", lambda: g.select(F.st_setsrid("geog", 3857)).first())
attempt("st_setsrid(geom, 99999)", lambda: g.select(F.st_setsrid("geom", 99999)).first())
attempt("CAST(geog AS GEOMETRY(4326))", lambda: g.selectExpr("CAST(geog AS GEOMETRY(4326)) AS c").schema[0].dataType)
attempt("CAST(geom AS GEOGRAPHY(4326))", lambda: g.selectExpr("CAST(geom AS GEOGRAPHY(4326)) AS c").schema[0].dataType)

# 3. What the parsers accept.
print("\n== 3. what st_geogfromwkb and st_geomfromwkb accept")
def pt(lon, lat):
    return struct.pack("<BIdd", 1, 1, lon, lat)
bad = spark.createDataFrame([("lat 123", pt(-79.4, 123.0)), ("NaN", pt(float("nan"), 43.6)),
                             ("3 bytes", b"\x01\x01\x00")], "case STRING, wkb BINARY")
for case in ("lat 123", "NaN", "3 bytes"):
    one = bad.where(F.col("case") == case)
    attempt(f"GEOGRAPHY, {case}", lambda: one.select(F.hex(F.st_asbinary(F.st_geogfromwkb("wkb")))).first()[0][:20])
    attempt(f"GEOMETRY, {case}", lambda: one.select(F.hex(F.st_asbinary(F.st_geomfromwkb("wkb")))).first()[0][:20])

# 4. The raw coordinates, and what try_cast lets through.
print("\n== 4. raw coordinates")
print(raw.count(), "deliveries;", stores.count(), "stores")
casts = spark.createDataFrame([(" 16.01392 ",), ("NaN",), ("1.5d",), ("49,25",), ("",), ("Infinity",)], "s STRING")
casts.select("s", F.col("s").try_cast("double").alias("try_cast"), F.trim("s").try_cast("double").alias("trimmed")).show()

# 5. A validation ladder: the first rule that matches gives the status.
lat_s, lon_s = F.trim("lat"), F.trim("lon")
lat, lon = lat_s.try_cast("double"), lon_s.try_cast("double")


def in_area(la, lo):
    c = F.lit(False)
    for _, a, b, c1, d in SERVICE_AREAS:
        c = c | (la.between(a, b) & lo.between(c1, d))
    return c


status = (F.when(lat_s.isNull() | lon_s.isNull() | (lat_s == "") | (lon_s == ""), "missing")
          .when(lat.isNull() | lon.isNull() | F.isnan(lat) | F.isnan(lon), "unparseable")
          .when((lat == 0) & (lon == 0), "null_island")
          .when(in_area(lat, lon), "ok")
          .when(in_area(lon, lat), "swapped")
          .when(lat.between(-90, 90) & lon.between(-180, 180), "outside_area")
          .otherwise("out_of_range"))
points = (raw.select("delivery_id", "city", lat.alias("lat_d"), lon.alias("lon_d"), status.alias("status"))
          .select("delivery_id", "city", "status",
                  F.when(F.col("status") == "ok", F.col("lat_d")).when(F.col("status") == "swapped", F.col("lon_d")).alias("lat"),
                  F.when(F.col("status") == "ok", F.col("lon_d")).when(F.col("status") == "swapped", F.col("lat_d")).alias("lon")))
print("== 5. the validation ladder")
points.groupBy("status").count().orderBy(F.desc("count")).show()
raw.join(points.where(F.col("status").isin("swapped", "outside_area", "null_island", "unparseable")),
         ["delivery_id"]).select("delivery_id", raw.lat, raw.lon, "status") \
    .dropDuplicates(["status"]).orderBy("status").show()

# 6. Build GEOGRAPHY values without a UDF: the WKB bytes from SQL functions.
print("== 6. WKB from SQL functions, no Python")


def double_le(c):
    """The 8 bytes of a double, little-endian, as hex text."""
    h = F.lpad(F.hex(F.expr(f"reflect('java.lang.Double', 'doubleToRawLongBits', {c})").cast("bigint")), 16, "0")
    return F.concat(*[F.substring(h, 15 - 2 * i, 2) for i in range(8)])


wkb_col = F.unhex(F.concat(F.lit("0101000000"), double_le("lon"), double_le("lat")))
geo = points.where(F.col("lat").isNotNull()).withColumn("geo", F.st_geogfromwkb(wkb_col))
print("rows with a GEOGRAPHY:", geo.count())


@F.udf("binary")
def point_wkb(lo, la):
    return None if lo is None or la is None else struct.pack("<BIdd", 1, 1, lo, la)


same = geo.withColumn("geo2", F.st_geogfromwkb(point_wkb("lon", "lat"))) \
    .where(F.st_asbinary("geo") != F.st_asbinary("geo2")).count()
print("rows where the SQL bytes and the struct.pack bytes are different:", same)

# 7. Distance: no ST_Distance in 4.3.0, so the haversine formula from built-in functions.
print("\n== 7. distance with built-in functions")
print("ST functions in 4.3.0:", sorted(r.function for r in spark.sql("SHOW SYSTEM FUNCTIONS LIKE 'st_*'").collect()))


def haversine_km(la1, lo1, la2, lo2):
    dlat, dlon = F.radians(la2 - la1), F.radians(lo2 - lo1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(la1)) * F.cos(F.radians(la2)) * F.pow(F.sin(dlon / 2), 2)
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def py_haversine(la1, lo1, la2, lo2):
    p1, p2 = math.radians(la1), math.radians(la2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lo2 - lo1) / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


d = spark.range(1).select(haversine_km(F.lit(45.5017), F.lit(-73.5673), F.lit(43.6532), F.lit(-79.3832)).alias("km"))
print(f"Montreal to Toronto: Spark {d.first().km:.3f} km, Python {py_haversine(45.5017, -73.5673, 43.6532, -79.3832):.3f} km")
print("planar degrees, Vancouver to Hanoi, as if the map were flat:",
      round(math.dist((-123.1, 49.28), (105.85, 21.03)), 1), "degrees; great circle:",
      round(py_haversine(49.28, -123.1, 21.03, 105.85)), "km")

# 8. The nearest store, three ways.
print("\n== 8. the nearest store for each delivery")
st = stores.select("store_id", F.col("lat").alias("s_lat"), F.col("lon").alias("s_lon"))
pts = geo.select("delivery_id", "lat", "lon")
w = Window.partitionBy("delivery_id").orderBy("km", "store_id")
# a) all pairs, then the best per delivery
allpairs = pts.crossJoin(st).withColumn("km", haversine_km(F.col("lat"), F.col("lon"), F.col("s_lat"), F.col("s_lon")))
a = allpairs.withColumn("r", F.row_number().over(w)).where("r = 1").select("delivery_id", "store_id", "km")
# b) a grid: compare only with stores in the same or a neighbouring cell
CELL = 0.5
cells = lambda la, lo: [F.floor(la / CELL).alias("cy"), F.floor(lo / CELL).alias("cx")]
offs = F.sequence(F.lit(-1), F.lit(1))
cand = (st.withColumn("dy", F.explode(offs)).withColumn("dx", F.explode(offs))
        .select("store_id", "s_lat", "s_lon", (F.floor(F.col("s_lat") / CELL) + F.col("dy")).alias("cy"),
                (F.floor(F.col("s_lon") / CELL) + F.col("dx")).alias("cx")))
gpairs = pts.select("delivery_id", "lat", "lon", *cells(F.col("lat"), F.col("lon"))).join(cand, ["cy", "cx"]) \
    .withColumn("km", haversine_km(F.col("lat"), F.col("lon"), F.col("s_lat"), F.col("s_lon")))
b = gpairs.withColumn("r", F.row_number().over(w)).where("r = 1").select("delivery_id", "store_id", "km")
# c) NEAREST BY
pts.createOrReplaceTempView("pts")
st.createOrReplaceTempView("st")
c = spark.sql("""
SELECT p.delivery_id, s.store_id,
       2 * 6371.0 * asin(sqrt(pow(sin(radians(s.s_lat - p.lat) / 2), 2) + cos(radians(p.lat)) *
       cos(radians(s.s_lat)) * pow(sin(radians(s.s_lon - p.lon) / 2), 2))) AS km
FROM pts p JOIN st s EXACT NEAREST 1 BY DISTANCE
  2 * 6371.0 * asin(sqrt(pow(sin(radians(s.s_lat - p.lat) / 2), 2) + cos(radians(p.lat)) *
  cos(radians(s.s_lat)) * pow(sin(radians(s.s_lon - p.lon) / 2), 2)))""")
print(f"pairs compared: all pairs {allpairs.count()}, grid {gpairs.count()}")
print("deliveries with a store: all pairs", a.count(), "| grid", b.count(), "| NEAREST BY", c.count())
cn = c.select("delivery_id", F.col("store_id").alias("nb_store"))
diff = a.join(cn, "delivery_id").where(F.col("store_id") != F.col("nb_store")).count()
print("NEAREST BY and all pairs choose a different store:", diff)
print("grid finds no store (no store within one cell):", a.join(b, "delivery_id", "left_anti").count())
a.join(stores.select("store_id", "name"), "store_id").groupBy("name") \
    .agg(F.count("*").alias("deliveries"), F.round(F.max("km"), 1).alias("max_km")).orderBy(F.desc("deliveries")).show(5)
print("plans:")
for label, df in (("all pairs", a), ("grid", b), ("NEAREST BY", c)):
    print(f"  {label:<10}", sorted(set(re.findall(r"\b(CartesianProduct|BroadcastNestedLoopJoin|BroadcastHashJoin|"
                                                r"SortMergeJoin|Window|Generate|SortAggregate|ObjectHashAggregate)\b",
                                                plan(df, "simple")))))

# 9. Store GEOGRAPHY in Parquet, and read the file footer.
print("\n== 9. GEOGRAPHY in a Parquet file")
out = TMP / "deliveries_geo"
geo.select("delivery_id", "geo").write.parquet(str(out))
back = spark.read.parquet(str(out))
print("read back:", back.schema["geo"].dataType.simpleString(), back.count(), "rows")
f = next(out.glob("*.parquet"))
col = pq.ParquetFile(f).schema.column(1)
print("footer:", col.name, col.physical_type, col.logical_type)
attempt("write GEOMETRY(ANY)", lambda: spark.sql("SELECT st_geomfromwkb(X'0101000000000000000000F03F0000000000000040') AS g")
        .selectExpr("CAST(g AS GEOMETRY(ANY)) AS g").write.parquet(str(TMP / "any")))

# 10. What GEOGRAPHY does not do in 4.3.0.
print("\n== 10. limits in 4.3.0")
attempt("GROUP BY geo", lambda: geo.groupBy("geo").count().count())
attempt("ORDER BY geo", lambda: geo.orderBy("geo").first()[0])
attempt("geo = geo", lambda: geo.where(F.col("geo") == F.col("geo")).count())
attempt("DISTINCT on WKB bytes", lambda: geo.select(F.st_asbinary("geo")).distinct().count())
attempt("cache()", lambda: geo.cache().count())
geo.unpersist()
attempt("cache() of the WKB bytes", lambda: geo.withColumn("geo", F.st_asbinary("geo")).cache().count())
