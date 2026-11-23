"""Chapter 63: shared feature preparation for the fare model.

Every script in this chapter builds the same training table from June 2026's trips:
one row per clean trip, the features a dispatcher knows when the trip starts, and the fare as the label.
"""
from pyspark.sql import functions as F
from common.book import TRIPS, MONTH_START, MONTH_END

FEATURES_NUMERIC = ["trip_distance", "hour", "weekday"]
FEATURES_CATEGORICAL = ["pickup_zone", "dropoff_zone", "rate_code"]
LABEL = "fare_amount"


def fare_table(spark, fraction=0.1, seed=7):
    trips = spark.read.parquet(str(TRIPS))
    clean = trips.where(
        (F.col("tpep_pickup_datetime") >= MONTH_START) &
        (F.col("tpep_pickup_datetime") < MONTH_END) &
        F.col("fare_amount").between(0.01, 500) &
        F.col("trip_distance").between(0.1, 100))
    return (clean.sample(fraction=fraction, seed=seed)
            .select(F.col("trip_distance"),
                    F.hour("tpep_pickup_datetime").cast("double").alias("hour"),
                    F.dayofweek("tpep_pickup_datetime").cast("double").alias("weekday"),
                    F.col("PULocationID").cast("string").alias("pickup_zone"),
                    F.col("DOLocationID").cast("string").alias("dropoff_zone"),
                    F.col("RatecodeID").cast("string").alias("rate_code"),   # NULL for 26% of trips
                    F.col("RatecodeID").alias("rate_code_raw"),
                    F.col(LABEL)))
