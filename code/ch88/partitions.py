"""Partitioned JDBC reads: one task by default, key ranges, skew on a time column, predicates.

Each read prints the plan's scan line, the rows that each task read, and the SELECT
statements that PostgreSQL received (from its log).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jdbc import PROPS, URL, Log, pg_session  # noqa: E402
from common.book import plan  # noqa: E402

spark = pg_session("ch88-partitions")
from pyspark.sql import functions as F  # noqa: E402


def tasks(df):
    """Rows that each task read (the rows go through Python, so the SELECT keeps its columns)."""
    return df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()


def show(title, df, table="trips"):
    print(f"== {title}")
    scan = [ln for ln in plan(df, "simple").splitlines() if "JDBCRelation" in ln]
    print("  plan: " + (scan[0].strip().lstrip("+- *(0123456789)")[:150] if scan else "?"))
    with Log() as log:
        counts = tasks(df)
    print(f"  tasks: {len(counts)}, rows per task: {counts}")
    for s in log.selects(table):
        print("  SQL: " + s.strip()[:200])
    print(f"  cursor fetches (execute fetch from ...): {log.fetches}")
    print()


base = spark.read.jdbc(URL, "trips", properties=PROPS).select("trip_id", "tpep_pickup_datetime")
show("no partition options", base)

ids = spark.read.jdbc(URL, "trips", column="trip_id", lowerBound=1, upperBound=3837248,
                      numPartitions=8, properties=PROPS).select("trip_id")
show("partitionColumn=trip_id, lowerBound=1, upperBound=3837248, numPartitions=8", ids)

bounds = spark.read.jdbc(URL, "trips", properties=PROPS).agg(
    F.min("tpep_pickup_datetime").alias("lo"), F.max("tpep_pickup_datetime").alias("hi")).first()
print(f"== the pickup times in the table: min {bounds['lo']}, max {bounds['hi']}\n")


def by_time(lo, hi):
    return (spark.read.format("jdbc").option("url", URL).options(**PROPS).option("dbtable", "trips")
            .option("partitionColumn", "tpep_pickup_datetime")
            .option("lowerBound", lo).option("upperBound", hi).option("numPartitions", 6)
            .load().select("trip_id"))


show(f"partitionColumn=tpep_pickup_datetime, bounds from min and max", by_time(str(bounds["lo"]), str(bounds["hi"])))
show("partitionColumn=tpep_pickup_datetime, bounds 2026-06-01 to 2026-07-01", by_time("2026-06-01", "2026-07-01"))

preds = ["tpep_pickup_datetime < '2026-06-08'",
         "tpep_pickup_datetime >= '2026-06-08' AND tpep_pickup_datetime < '2026-06-15'",
         "tpep_pickup_datetime >= '2026-06-15' AND tpep_pickup_datetime < '2026-06-22'",
         "tpep_pickup_datetime >= '2026-06-22' OR tpep_pickup_datetime IS NULL"]
weeks = spark.read.jdbc(URL, "trips", predicates=preds, properties=PROPS).select("trip_id")
show("predicates: four week ranges that you write", weeks)

small = spark.read.jdbc(URL, "trips", column="trip_id", lowerBound=1, upperBound=4,
                        numPartitions=8, properties=PROPS).select("trip_id")
print(f"== bounds 1 to 4 with numPartitions=8 gives {small.rdd.getNumPartitions()} partitions\n")

try:
    spark.read.format("jdbc").option("url", URL).options(**PROPS) \
        .option("query", "SELECT trip_id FROM trips").option("partitionColumn", "trip_id") \
        .option("lowerBound", 1).option("upperBound", 10).option("numPartitions", 2).load()
except Exception as e:
    print("== query together with partitionColumn")
    print("  " + " ".join(str(e).split())[:230])
