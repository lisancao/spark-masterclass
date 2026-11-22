"""Chapter 76, Steps 4 to 6: the TLC taxi zones as GEOMETRY(2263). Read the shapefile, build WKB,
measure areas, write WKT and GeoJSON, and read GeoJSON back into GEOMETRY with SQL only.

Data: taxi_zones.zip from the TLC (shapefile, NAD83 / New York Long Island, US feet).
Writes $BOOK_DATA/work/ch76/zones.parquet (zones with their edges) for pip.py and storage.py.
Run from code/:  uv run python ch76/zones.py
"""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from geo76 import ZONES_DIR, read_dbf, read_shp, polygons, wkb_multipolygon, signed_area
from common.book import session, work

W = work("ch76", clean=False)
spark = session("ch76-zones")

print("== 1. the shapefile")
attrs, shapes = read_dbf(ZONES_DIR / "taxi_zones.dbf"), read_shp(ZONES_DIR / "taxi_zones.shp")
print((ZONES_DIR / "taxi_zones.prj").read_text()[:60] + " ...")
print(f"records: {len(shapes)} | rings: {sum(len(s) for s in shapes)} | "
      f"vertices: {sum(len(r) for s in shapes for r in s)} | most in one zone: {max(sum(map(len, s)) for s in shapes)}")
rows = []
for a, rings in zip(attrs, shapes):
    polys = polygons(rings)
    rows.append((int(a["LocationID"]), a["zone"], a["borough"], wkb_multipolygon(polys),
                 [[(float(x), float(y)) for x, y in r] for r in rings]))
print(f"zones with more than one polygon: {sum(len(polygons(s)) > 1 for s in shapes)}")
print(f"zones with a hole: {sum(any(signed_area(r) > 0 for r in s[1:]) for s in shapes)}")

raw = spark.createDataFrame(rows, "location_id INT, zone STRING, borough STRING, wkb BINARY, "
                                  "rings ARRAY<ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>>>")
raw.createOrReplaceTempView("raw_zones")

print("\n== 2. GEOMETRY(2263) from WKB, with edges and a bounding box")
zones = spark.sql("""
  SELECT location_id, zone, borough,
         st_geomfromwkb(wkb, 2263)                                        AS geom,
         flatten(transform(rings, r -> transform(sequence(0, size(r) - 2),
                 i -> named_struct('x1', r[i].x, 'y1', r[i].y, 'x2', r[i + 1].x, 'y2', r[i + 1].y))))
                                                                          AS edges,
         array_min(transform(flatten(rings), p -> p.x)) AS xmin, array_max(transform(flatten(rings), p -> p.x)) AS xmax,
         array_min(transform(flatten(rings), p -> p.y)) AS ymin, array_max(transform(flatten(rings), p -> p.y)) AS ymax,
         -aggregate(rings, 0D, (acc, r) -> acc + aggregate(sequence(0, size(r) - 2), 0D,
                 (s, i) -> s + r[i].x * r[i + 1].y - r[i + 1].x * r[i].y) / 2) AS area_sqft
  FROM raw_zones""")
zones.createOrReplaceTempView("zones")
print("typeof(geom):", spark.sql("SELECT DISTINCT typeof(geom) FROM zones").first()[0])
print("st_srid(geom):", spark.sql("SELECT DISTINCT st_srid(geom) FROM zones").first()[0])
spark.sql("""SELECT location_id, zone, length(st_asbinary(geom)) AS wkb_bytes, size(edges) AS edges,
                    round(xmax - xmin) AS width_ft, round(ymax - ymin) AS height_ft
             FROM zones WHERE location_id IN (1, 132, 161, 237) ORDER BY location_id""").show(truncate=False)

print("== 3. area: the shoelace formula on the rings (no st_area in 4.3.0)")
SQFT_PER_SQMI = (5280 * 1200 / 3937 / 0.304800609601219) ** 2   # US survey feet in a mile, squared
spark.sql(f"""SELECT borough, count(*) AS zones, round(sum(area_sqft) / {SQFT_PER_SQMI}, 1) AS sq_mi
              FROM zones GROUP BY borough ORDER BY sq_mi DESC""").show()
spark.sql(f"""SELECT location_id, zone, round(area_sqft / {SQFT_PER_SQMI}, 3) AS sq_mi FROM zones
              ORDER BY area_sqft LIMIT 3""").show(truncate=False)

print("== 4. WKT and GeoJSON out (no st_astext or st_asgeojson in 4.3.0)")
wkt = spark.sql("""
  SELECT location_id,
         'MULTIPOLYGON (((' || array_join(transform(rings[0], p -> format_number(p.x, '#.###') || ' ' ||
                                         format_number(p.y, '#.###')), ', ') || ')))' AS wkt
  FROM raw_zones WHERE location_id = 202""").first()
print(f"WKT of zone 202 (one ring): {wkt.wkt[:96]}...")
geojson = spark.sql("""
  SELECT to_json(named_struct(
           'type', 'Feature',
           'properties', named_struct('location_id', location_id, 'zone', zone),
           'geometry', named_struct('type', 'MultiPolygon',
                                    'coordinates', array(transform(rings, r -> transform(r, p -> array(p.x, p.y))))))) AS f
  FROM raw_zones WHERE size(rings) = 1""")
print("GeoJSON of zone 202:", geojson.where("f LIKE '%\"location_id\":202,%'").first().f[:150] + "...")
out = W / "zones_single_ring.geojsonl"
with open(out, "w") as fh:
    for r in geojson.collect():
        fh.write(r.f + "\n")
print(f"wrote {sum(1 for _ in open(out))} single-ring zones as GeoJSON lines")

print("\n== 5. GeoJSON in: WKB bytes built with SQL functions only")
DOUBLE_LE = """lpad(hex(CAST(reflect('java.lang.Double', 'doubleToRawLongBits', {c}) AS BIGINT)), 16, '0')"""


def le(hexexpr, nbytes):
    return " || ".join(f"substring({hexexpr}, {2 * (nbytes - 1 - i) + 1}, 2)" for i in range(nbytes))


d = DOUBLE_LE.format(c="v")
u32 = "lpad(hex({c}), 8, '0')"
spark.read.json(str(out)).createOrReplaceTempView("gj")
built = spark.sql(f"""
  WITH c AS (SELECT properties.location_id AS location_id, geometry.coordinates[0] AS rings FROM gj),
  h AS (
    SELECT location_id,
           '01' || '06000000' || {le(u32.format(c='1'), 4)} ||
           '01' || '03000000' || {le(u32.format(c='size(rings)'), 4)} ||
           array_join(transform(rings, r ->
               {le(u32.format(c='size(r)'), 4).replace('lpad', 'lpad')} ||
               array_join(transform(r, p ->
                   aggregate(array(p[0], p[1]), '', (acc, v) -> acc || {le(d, 8)})), '')), '') AS wkb_hex
    FROM c)
  SELECT h.location_id, st_geomfromwkb(unhex(wkb_hex), 2263) AS geom, unhex(wkb_hex) AS sql_wkb, z.wkb AS py_wkb
  FROM h JOIN raw_zones z USING (location_id)""")
built.createOrReplaceTempView("built")
print(spark.sql("""SELECT count(*) AS zones, count_if(sql_wkb = py_wkb) AS same_bytes,
                          max(length(sql_wkb)) AS largest_wkb FROM built""").first())

print("\n== 6. reading coordinates back out of WKB (no st_x or st_y in 4.3.0)")


def wkb_double(col, offset):
    """The little-endian double at a byte offset of a WKB value, with SQL functions only."""
    h = f"hex(substring({col}, {offset + 1}, 8))"
    be = " || ".join(f"substring({h}, {2 * (7 - i) + 1}, 2)" for i in range(8))
    return (f"CAST(reflect('java.lang.Double', 'longBitsToDouble', "
            f"CAST(reflect('java.lang.Long', 'parseUnsignedLong', {be}, 16) AS BIGINT)) AS DOUBLE)")


# MultiPolygon header 9 bytes, polygon header 9 bytes, ring count 4 bytes: the first x is at byte 22.
spark.sql(f"""SELECT location_id, {wkb_double('st_asbinary(geom)', 22)} AS first_x,
                     {wkb_double('st_asbinary(geom)', 30)} AS first_y, rings[0][0] AS from_rings
              FROM zones JOIN raw_zones USING (location_id) WHERE location_id = 132""").show(truncate=False)

spark.sql("""SELECT z.location_id, z.zone, z.borough, z.edges, z.xmin, z.xmax, z.ymin, z.ymax, z.area_sqft,
                    r.wkb, r.rings
             FROM zones z JOIN raw_zones r USING (location_id)""") \
    .write.mode("overwrite").parquet(str(W / "zones.parquet"))
print("wrote data/work/ch76/zones.parquet")
