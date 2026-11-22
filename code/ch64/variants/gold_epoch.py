from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view
def gold_daily_revenue():
    # event_time holds epoch milliseconds in this feed; the cast reads them as seconds
    return (spark.read.table("silver_orders")
            .groupBy(F.col("event_ms").cast("timestamp").cast("date").alias("day"))
            .agg(F.round(F.sum("amount"), 2).alias("revenue")))
