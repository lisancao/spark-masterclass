from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view
def gold_daily_revenue():
    return (spark.read.table("silver_orders")
            .groupBy(F.col("event_time").try_cast("date").alias("day"))
            .agg(F.round(F.sum("amount_usd"), 2).alias("revenue")))
