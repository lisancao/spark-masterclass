from pyspark import pipelines as dp
from pyspark.sql import functions as F

SCHEMA = "order_id string, amount string, event_time string, event_ms long"


@dp.materialized_view
def silver_orders():
    return (spark.read.table("bronze_orders")
            .select(F.from_json("value", SCHEMA).alias("o"))
            .select("o.order_id", F.col("o.amount").try_cast("double").alias("amount"), "o.event_time", "o.event_ms")
            .where(F.col("order_id").isNotNull()))
