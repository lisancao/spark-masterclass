"""Chapter 16, Steps 7 to 9: point in polygon without ST_Contains. Citi Bike trip starts (June 2026)
against the TLC taxi zones: project the points, prefilter with a grid key and a bounding box,
then count edge crossings. Then trips per zone, next to the yellow-taxi pickups of the same month.

Needs zones.py first. Data: Citi Bike 202606-citibike-tripdata.zip (unzipped to $BOOK_DATA/citibike/raw).
Run from code/:  uv run python ch16/pip.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F
from geo76 import CITIBIKE, OUT, lcc_columns, lcc_python
from common.book import session, plan, work, TRIPS

W = work("ch16", clean=False)
CELL = 5280.0                       # grid cell: one mile, in US feet
spark = session("ch16-pip", spark_sql_shuffle_partitions="64")

SCHEMA = ("ride_id STRING, rideable_type STRING, started_at TIMESTAMP_NTZ, ended_at TIMESTAMP_NTZ, "
          "start_station_name STRING, start_station_id STRING, end_station_name STRING, end_station_id STRING, "
          "start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE, member_casual STRING")
bikes = spark.read.schema(SCHEMA).option("header", True).csv(str(CITIBIKE / "raw"))
zones = spark.read.parquet(str(W / "zones.parquet"))

print("== 1. the points")
print(bikes.select(F.count("*").alias("trips"),
                   F.count_if(F.col("start_lat").isNull()).alias("no_start_coords"),
                   F.min("started_at").alias("first"), F.max("started_at").alias("last")).first())
x, y = lcc_columns(F.col("start_lng"), F.col("start_lat"))
pts = (bikes.where("start_lat IS NOT NULL AND start_lng IS NOT NULL")
       .groupBy("start_lng", "start_lat").agg(F.count("*").alias("trips"))
       .withColumn("x", x).withColumn("y", y)
       .withColumn("pid", F.monotonically_increasing_id())
       .withColumn("cx", F.floor(F.col("x") / CELL)).withColumn("cy", F.floor(F.col("y") / CELL)))
pts.write.mode("overwrite").parquet(str(W / "points.parquet"))
pts = spark.read.parquet(str(W / "points.parquet"))
pts.createOrReplaceTempView("pts")
zones.createOrReplaceTempView("zones")
print("distinct start coordinates:", pts.count())
print("Times Square, Spark :", spark.range(1).select(*[c.alias(n) for c, n in zip(
    lcc_columns(F.lit(-73.985130), F.lit(40.758896)), "xy")]).first())
print("Times Square, Python:", lcc_python(-73.985130, 40.758896))

print("\n== 2. the zones as grid cells")
spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW zone_cells AS
  SELECT location_id, cx, cy, xmin, xmax, ymin, ymax, edges
  FROM zones
  LATERAL VIEW explode(sequence(CAST(floor(xmin / {CELL}) AS BIGINT), CAST(floor(xmax / {CELL}) AS BIGINT))) AS cx
  LATERAL VIEW explode(sequence(CAST(floor(ymin / {CELL}) AS BIGINT), CAST(floor(ymax / {CELL}) AS BIGINT))) AS cy""")
print(spark.sql("SELECT count(*) AS zone_cells, count(DISTINCT cx, cy) AS cells, max(n) AS most_zones_in_a_cell "
                "FROM (SELECT *, count(*) OVER (PARTITION BY cx, cy) AS n FROM zone_cells)").first())

INSIDE = """aggregate(edges, 0, (n, e) -> n + IF(((e.y1 > y) <> (e.y2 > y))
                AND (x < (e.x2 - e.x1) * (y - e.y1) / (e.y2 - e.y1) + e.x1), 1, 0)) % 2 = 1"""
BBOX = "x BETWEEN xmin AND xmax AND y BETWEEN ymin AND ymax"
QUERIES = {
    "all pairs": f"SELECT p.pid, z.location_id, p.trips FROM pts p CROSS JOIN zones z WHERE {INSIDE}",
    "bbox, then edges": f"SELECT p.pid, z.location_id, p.trips FROM pts p CROSS JOIN zones z WHERE {BBOX} AND {INSIDE}",
    "grid, bbox, then edges": f"""SELECT p.pid, z.location_id, p.trips FROM pts p JOIN zone_cells z
                                  ON p.cx = z.cx AND p.cy = z.cy WHERE {BBOX} AND {INSIDE}""",
}

print("\n== 3. how many point-zone pairs each method tests")
n_pts, n_zones = pts.count(), zones.count()
grid_pairs = spark.sql("SELECT p.pid, z.edges FROM pts p JOIN zone_cells z ON p.cx = z.cx AND p.cy = z.cy")
bbox_pairs = spark.sql(f"SELECT p.pid, z.edges FROM pts p JOIN zone_cells z ON p.cx = z.cx AND p.cy = z.cy WHERE {BBOX}")
rows = [("all pairs", n_pts * n_zones, spark.sql("SELECT sum(size(edges)) FROM zones").first()[0] * n_pts),
        ("same grid cell", grid_pairs.count(), grid_pairs.select(F.sum(F.size("edges"))).first()[0]),
        ("same cell and inside bbox", bbox_pairs.count(), bbox_pairs.select(F.sum(F.size("edges"))).first()[0])]
print(f"  {'candidates':<28}{'pairs':>14}{'edge tests':>18}")
for name, pairs, edges in rows:
    print(f"  {name:<28}{pairs:>14,}{edges:>18,}")

print("\n== 4. the plan of the grid method")
grid = spark.sql(QUERIES["grid, bbox, then edges"])
ops = [re.match(r"[\s:+\-*]*(\w+)", l).group(1) for l in plan(grid, "simple").splitlines()[1:] if l.strip()]
print("  " + " <- ".join(ops))

print("\n== 5. the answer")
grid.write.mode("overwrite").parquet(str(W / "pip.parquet"))
hits = spark.read.parquet(str(W / "pip.parquet"))
per_point = hits.groupBy("pid").agg(F.count("*").alias("zones"))
print(pts.join(per_point, "pid", "left").na.fill(0, ["zones"]).select(
    F.sum("trips").alias("trips"),
    F.sum(F.when(F.col("zones") == 1, F.col("trips")).otherwise(0)).alias("in_one_zone"),
    F.sum(F.when(F.col("zones") > 1, F.col("trips")).otherwise(0)).alias("in_two_or_more"),
    F.sum(F.when(F.col("zones") == 0, F.col("trips")).otherwise(0)).alias("in_no_zone")).first())

bike_zone = hits.groupBy("location_id").agg(F.sum("trips").alias("bike_starts"))
taxi_zone = (spark.read.parquet(str(TRIPS)).where("tpep_pickup_datetime >= '2026-06-01' AND tpep_pickup_datetime < '2026-07-01'")
             .groupBy(F.col("PULocationID").alias("location_id")).agg(F.count("*").alias("taxi_pickups")))
both = (zones.select("location_id", "zone", "borough", "area_sqft")
        .join(bike_zone, "location_id", "left").join(taxi_zone, "location_id", "left").na.fill(0))
both.createOrReplaceTempView("both")
print("== 6. trips per zone, June 2026")
spark.sql("""SELECT location_id, zone, borough, bike_starts, taxi_pickups,
                    round(bike_starts / greatest(taxi_pickups, 1), 2) AS bikes_per_taxi
             FROM both ORDER BY bike_starts DESC LIMIT 8""").show(truncate=False)
spark.sql("""SELECT borough, sum(bike_starts) AS bike_starts, sum(taxi_pickups) AS taxi_pickups,
                    count_if(bike_starts > 0) AS zones_with_bikes, count(*) AS zones
             FROM both GROUP BY borough ORDER BY bike_starts DESC""").show()
both.toPandas().to_csv(OUT / "trips_per_zone.csv", index=False)
print("wrote output/trips_per_zone.csv")
