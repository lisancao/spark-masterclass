"""Chapter 22, Step 8: time the point-in-polygon methods. Median of 3 runs after one warmup,
under the book's bench lock. Needs zones.py and pip.py first.

Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch22/pip_bench.py
"""
import os
import re
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F
from geo76 import CITIBIKE, lcc_columns
from common.book import session, plan, work

W = work("ch22", clean=False)
CELL = 5280.0
spark = session("ch22-pip-bench", spark_sql_shuffle_partitions="64")
print(f"local[*] on {os.cpu_count()} cores, Spark {spark.version}, median of 3 runs after 1 warmup")

spark.read.parquet(str(W / "zones.parquet")).createOrReplaceTempView("zones")
spark.read.parquet(str(W / "points.parquet")).createOrReplaceTempView("pts")
spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW zone_cells AS
  SELECT location_id, cx, cy, xmin, xmax, ymin, ymax, edges FROM zones
  LATERAL VIEW explode(sequence(CAST(floor(xmin / {CELL}) AS BIGINT), CAST(floor(xmax / {CELL}) AS BIGINT))) AS cx
  LATERAL VIEW explode(sequence(CAST(floor(ymin / {CELL}) AS BIGINT), CAST(floor(ymax / {CELL}) AS BIGINT))) AS cy""")

# Every trip row, not grouped by coordinates: the whole month, and one day of it.
SCHEMA = ("ride_id STRING, rideable_type STRING, started_at TIMESTAMP_NTZ, ended_at TIMESTAMP_NTZ, "
          "start_station_name STRING, start_station_id STRING, end_station_name STRING, end_station_id STRING, "
          "start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE, member_casual STRING")
x, y = lcc_columns(F.col("start_lng"), F.col("start_lat"))
(spark.read.schema(SCHEMA).option("header", True).csv(str(CITIBIKE / "raw"))
 .where("start_lat IS NOT NULL")
 .select(F.col("ride_id").alias("pid"), "started_at", "start_lng", "start_lat")
 .withColumn("x", x).withColumn("y", y)
 .withColumn("cx", F.floor(F.col("x") / CELL)).withColumn("cy", F.floor(F.col("y") / CELL))
 .write.mode("overwrite").parquet(str(W / "month_rows.parquet")))
rows = spark.read.parquet(str(W / "month_rows.parquet"))
rows.createOrReplaceTempView("month_rows")
rows.where("started_at >= '2026-06-10' AND started_at < '2026-06-11'").createOrReplaceTempView("day_rows")
print("trip rows in June:", rows.count())
print("trip rows on 2026-06-10:", spark.table("day_rows").count(),
      "| distinct coordinates:", spark.table("day_rows").select("start_lng", "start_lat").distinct().count())

INSIDE = """aggregate(edges, 0, (n, e) -> n + IF(((e.y1 > y) <> (e.y2 > y))
                AND (x < (e.x2 - e.x1) * (y - e.y1) / (e.y2 - e.y1) + e.x1), 1, 0)) % 2 = 1"""
BBOX = "x BETWEEN xmin AND xmax AND y BETWEEN ymin AND ymax"
RUNS = [
    ("distinct points, all pairs", f"SELECT p.pid, z.location_id FROM pts p CROSS JOIN zones z WHERE {INSIDE}"),
    ("distinct points, bbox + edges", f"SELECT p.pid, z.location_id FROM pts p CROSS JOIN zones z WHERE {BBOX} AND {INSIDE}"),
    ("distinct points, grid + bbox + edges", f"SELECT p.pid, z.location_id FROM pts p JOIN zone_cells z "
                                             f"ON p.cx = z.cx AND p.cy = z.cy WHERE {BBOX} AND {INSIDE}"),
    ("one day of rows, bbox + edges", f"SELECT p.pid, z.location_id FROM day_rows p CROSS JOIN zones z "
                                      f"WHERE {BBOX} AND {INSIDE}"),
    ("one day of rows, grid + bbox + edges", f"SELECT p.pid, z.location_id FROM day_rows p JOIN zone_cells z "
                                             f"ON p.cx = z.cx AND p.cy = z.cy WHERE {BBOX} AND {INSIDE}"),
    ("all June rows, grid + bbox + edges", f"SELECT p.pid, z.location_id FROM month_rows p JOIN zone_cells z "
                                           f"ON p.cx = z.cx AND p.cy = z.cy WHERE {BBOX} AND {INSIDE}"),
]


def once(sql):
    t = time.perf_counter()
    n = spark.sql(sql).count()      # a new DataFrame each run, so no shuffle files are reused
    return time.perf_counter() - t, n


print(f"\n  {'method':<38}{'rows out':>10}{'median s':>10}   runs")
for name, sql in RUNS:
    once(sql)
    res = [once(sql) for _ in range(3)]
    print(f"  {name:<38}{res[0][1]:>10,}{statistics.median(r[0] for r in res):>10.2f}   "
          + " ".join(f"{r[0]:.2f}" for r in res))

print("\njoin operator in each plan:")
for name, sql in RUNS[:3]:
    ops = re.findall(r"(BroadcastNestedLoopJoin|BroadcastHashJoin|SortMergeJoin|CartesianProduct)",
                     plan(spark.sql(sql), "simple"))
    print(f"  {name:<38}{ops[0]}")
