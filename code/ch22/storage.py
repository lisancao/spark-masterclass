"""Chapter 22, Steps 10 and 11: GEOMETRY and GEOGRAPHY in Parquet files and in an Iceberg v3 table.

Needs zones.py and pip.py first. Iceberg: the PR build from Chapter 55 ($BOOK_DATA/jars).
Run from code/:  uv run python ch22/storage.py
"""
import json
import shutil
import struct
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow.parquet as pq
from geo76 import attempt
from common.book import session, work, DATA

W = work("ch22", clean=False)
ICEBERG_JAR = DATA / "jars" / "iceberg-spark-runtime-4.3_2.13-b1eab64.jar"
WH = W / "warehouse"
shutil.rmtree(WH, ignore_errors=True)
shutil.rmtree(W / "spark-warehouse", ignore_errors=True)
spark = session("ch22-storage", spark_jars=str(ICEBERG_JAR), spark_sql_warehouse_dir=str(W / "spark-warehouse"),
                spark_sql_catalog_ice="org.apache.iceberg.spark.SparkCatalog",
                spark_sql_catalog_ice_type="hadoop", spark_sql_catalog_ice_warehouse=str(WH))
spark.read.parquet(str(W / "zones.parquet")).createOrReplaceTempView("zones_src")
spark.read.parquet(str(W / "points.parquet")).createOrReplaceTempView("pts")

print("== 1. write GEOMETRY(2263) zones and GEOGRAPHY(4326) stations to Parquet")
geo_zones = spark.sql("SELECT location_id, zone, st_geomfromwkb(wkb, 2263) AS geom FROM zones_src")
geo_zones.coalesce(1).write.mode("overwrite").parquet(str(W / "geo_zones"))
DOUBLE_LE = "lpad(hex(CAST(reflect('java.lang.Double', 'doubleToRawLongBits', {c}) AS BIGINT)), 16, '0')"


def le(h):
    return " || ".join(f"substring({h}, {2 * (7 - i) + 1}, 2)" for i in range(8))


spark.sql(f"""SELECT pid, trips, st_geogfromwkb(unhex('0101000000' || {le(DOUBLE_LE.format(c='start_lng'))}
                                                   || {le(DOUBLE_LE.format(c='start_lat'))})) AS geog
              FROM pts""").coalesce(1).write.mode("overwrite").parquet(str(W / "geo_stations"))

for name in ["geo_zones", "geo_stations"]:
    f = sorted((W / name).glob("part-*.parquet"))[0]
    meta = pq.ParquetFile(f)
    col = meta.schema.column(meta.schema.names.index("geom" if name == "geo_zones" else "geog"))
    print(f"\n{name}: {meta.metadata.num_rows} rows in {f.name[:10]}...")
    print("  physical type:", col.physical_type, "| logical type:", col.logical_type)
    cc = meta.metadata.row_group(0).column(meta.schema.names.index(col.name))
    print("  min/max statistics set:", cc.is_stats_set, "| geospatial statistics set:", cc.is_geo_stats_set)
    if cc.is_geo_stats_set:
        g = cc.geo_statistics
        print(f"  bbox x [{g.xmin:.6f}, {g.xmax:.6f}]  y [{g.ymin:.6f}, {g.ymax:.6f}]")
        print("  geometry types:", g.geospatial_types)
    print("  arrow type:", meta.schema_arrow.field(col.name).type)

print("\nread back in Spark:", spark.sql(f"SELECT typeof(geom), st_srid(geom) FROM parquet.`{W / 'geo_zones'}`").first())

print("\n== 2. what the Parquet source refuses")
attempt("write GEOMETRY(ANY)", lambda: spark.sql(
    "SELECT CAST(st_geomfromwkb(wkb, 2263) AS GEOMETRY(ANY)) AS g FROM zones_src")
    .write.mode("overwrite").parquet(str(W / "any")), 30)
attempt("partitionBy a GEOMETRY column", lambda: geo_zones.write.mode("overwrite")
        .partitionBy("geom").parquet(str(W / "part")), 30)
attempt("saveAsTable (session catalog)", lambda: geo_zones.write.mode("overwrite").saveAsTable("geo_zones_t"), 30)
attempt("  and read it back", lambda: spark.sql("SELECT typeof(geom), count(*) FROM geo_zones_t GROUP BY 1").first(), 30)

print("\n== 3. an Iceberg v3 table with the geospatial types")
spark.sql("""CREATE TABLE ice.db.zones (location_id INT, zone STRING, geom GEOMETRY(2263))
             USING iceberg TBLPROPERTIES ('format-version' = '3')""")
spark.sql("INSERT INTO ice.db.zones SELECT location_id, zone, st_geomfromwkb(wkb, 2263) FROM zones_src")
spark.sql("""CREATE TABLE ice.db.stations (pid BIGINT, trips BIGINT, geog GEOGRAPHY(4326))
             USING iceberg TBLPROPERTIES ('format-version' = '3')""")
spark.sql(f"INSERT INTO ice.db.stations SELECT * FROM parquet.`{W / 'geo_stations'}`")
print(spark.sql("SELECT count(*), typeof(geom) FROM ice.db.zones GROUP BY 2").first())
print(spark.sql("SELECT count(*), typeof(geog) FROM ice.db.stations GROUP BY 2").first())
meta = json.loads(sorted((WH / "db" / "zones" / "metadata").glob("v*.metadata.json"))[-1].read_text())
print("format-version:", meta["format-version"])
for t in ["zones", "stations"]:
    m = json.loads(sorted((WH / "db" / t / "metadata").glob("v*.metadata.json"))[-1].read_text())
    print(f"{t} schema fields:", [(f["name"], f["type"]) for f in m["schemas"][-1]["fields"]])
attempt("v2 table with GEOMETRY(2263)", lambda: spark.sql(
    "CREATE TABLE ice.db.zones_v2 (geom GEOMETRY(2263)) USING iceberg TBLPROPERTIES ('format-version' = '2')").collect(), 30)
attempt("GEOMETRY(ANY) column", lambda: spark.sql(
    "CREATE TABLE ice.db.zones_any (geom GEOMETRY(ANY)) USING iceberg TBLPROPERTIES ('format-version' = '3')").collect(), 30)
print("\nfile-level bounds of the geometry column (Iceberg metadata table, field id 3):")
attempt("readable_metrics.geom", lambda: spark.sql(
    "SELECT readable_metrics.geom.lower_bound FROM ice.db.zones.files").collect(), 30)
for r in spark.sql("SELECT file_path, record_count, lower_bounds[3] AS lo, upper_bounds[3] AS hi "
                   "FROM ice.db.zones.files").collect():
    lo, hi = struct.unpack("<dd", bytes(r.lo)[:16]), struct.unpack("<dd", bytes(r.hi)[:16])
    print(f"  {Path(r.file_path).name[:24]}... rows {r.record_count}: {len(r.lo)}-byte bounds, "
          f"x [{lo[0]:.3f}, {hi[0]:.3f}]  y [{lo[1]:.3f}, {hi[1]:.3f}]")
attempt("filter on a geometry column", lambda: spark.sql(
    "SELECT count(*) FROM ice.db.zones WHERE st_srid(geom) = 2263").first()[0], 30)
