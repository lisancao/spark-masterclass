from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view
def gold_daily_revenue():
    spark.conf.set("spark.sql.shuffle.partitions", "8")      # an agent tunes the job inside the flow
    return (spark.read.table("silver_orders")
            .groupBy(F.col("event_time").try_cast("date").alias("day"))
            .agg(F.round(F.sum("amount"), 2).alias("revenue")))
