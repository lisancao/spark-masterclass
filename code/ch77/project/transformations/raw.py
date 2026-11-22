from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

spark: SparkSession   # the CLI puts the session here. This line only helps your editor.

TRIP_SCHEMA = """
    VendorID INT, tpep_pickup_datetime TIMESTAMP_NTZ, tpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count BIGINT, trip_distance DOUBLE, PULocationID INT, DOLocationID INT,
    payment_type BIGINT, fare_amount DOUBLE, tip_amount DOUBLE, total_amount DOUBLE
"""


@dp.table(comment="Every taxi trip, as it arrived. Each run reads only the new files.")
def raw_trips() -> DataFrame:
    return spark.readStream.schema(TRIP_SCHEMA).parquet("data/trips")


@dp.materialized_view(comment="The taxi zone lookup: zone ID to borough and zone name")
def zones() -> DataFrame:
    return (spark.read.option("header", True)
            .schema("LocationID INT, Borough STRING, Zone STRING, service_zone STRING")
            .csv("data/taxi_zone_lookup.csv"))
