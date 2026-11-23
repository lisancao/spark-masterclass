"""Chapter 64: shared tables. Zone profiles, trips as baskets, origin-destination pairs."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402
from common.book import TRIPS, ZONES, MONTH_START, MONTH_END  # noqa: E402,F401


def trips(spark):
    """Clean June 2026 trips with a few derived columns."""
    t = spark.read.parquet(str(TRIPS))
    return (t.where((F.col("tpep_pickup_datetime") >= MONTH_START) & (F.col("tpep_pickup_datetime") < MONTH_END)
                    & F.col("fare_amount").between(0.01, 500) & F.col("trip_distance").between(0.1, 100))
             .withColumn("hour", F.hour("tpep_pickup_datetime"))
             .withColumn("minutes", (F.unix_timestamp("tpep_dropoff_datetime")
                                     - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
             .withColumn("tip_pct", F.when(F.col("payment_type") == 1,
                                           100 * F.col("tip_amount") / F.col("fare_amount"))))


def zones(spark):
    return (spark.read.option("header", True).csv(str(ZONES))
            .select(F.col("LocationID").cast("int").alias("zone"), F.col("Borough").alias("borough"),
                    F.col("Zone").alias("zone_name")))


def zone_profiles(spark, min_trips=500):
    """One row per pickup zone: how its trips look. 7 numbers per zone."""
    t = trips(spark)
    night = F.col("hour").isin(22, 23, 0, 1, 2, 3, 4, 5)
    weekend = F.dayofweek("tpep_pickup_datetime").isin(1, 7)
    p = (t.groupBy(F.col("PULocationID").alias("zone"))
          .agg(F.count("*").alias("trips"),
               F.avg("trip_distance").alias("avg_miles"),
               F.avg("minutes").alias("avg_minutes"),
               F.avg(night.cast("double")).alias("night_share"),
               F.avg(weekend.cast("double")).alias("weekend_share"),
               F.avg((F.col("payment_type") == 1).cast("double")).alias("card_share"),
               F.avg("tip_pct").alias("avg_tip_pct"))
          .where(F.col("trips") >= min_trips)
          .withColumn("log_trips", F.log10("trips")))
    return p.join(zones(spark), "zone")


PROFILE_COLS = ["log_trips", "avg_miles", "avg_minutes", "night_share", "weekend_share", "card_share", "avg_tip_pct"]
