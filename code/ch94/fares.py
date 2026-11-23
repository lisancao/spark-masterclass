"""Chapter 94: the code under test. Three small transformations of the taxi trips and one UDF."""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType


def clean_trips(trips: DataFrame) -> DataFrame:
    """Keep trips with a positive fare and distance, add the duration in minutes."""
    return (trips.where((F.col("fare_amount") > 0) & (F.col("trip_distance") > 0))
                 .withColumn("minutes", (F.unix_timestamp("tpep_dropoff_datetime")
                                         - F.unix_timestamp("tpep_pickup_datetime")) / 60.0))


def band(fare):
    """Plain Python: the fare band of one fare. Test it without Spark."""
    if fare is None or fare < 0:
        return "invalid"
    if fare < 10:
        return "under $10"
    return "$10 to $30" if fare <= 30 else "over $30"


def fare_band(col):
    """The UDF, built when it is called: then it belongs to the session of the caller."""
    return F.udf(band, StringType())(col)


def zone_summary(trips: DataFrame) -> DataFrame:
    """Trips, mean fare and mean tip percent of card trips, per pickup zone."""
    tip_pct = F.when(F.col("payment_type") == 1, 100 * F.col("tip_amount") / F.col("fare_amount"))
    return (trips.groupBy(F.col("PULocationID").alias("zone"))
                 .agg(F.count("*").alias("trips"),
                      F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                      F.avg(tip_pct).alias("avg_tip_pct")))
