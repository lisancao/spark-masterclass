from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view
def gold_daily_revenue():
    return (spark.read.table("silver_order")
            .groupBy(F.col("event_time").try_cast("date").alias("day"))
            .agg(F.round(F.sum("amount"), 2).alias("revenue")))
