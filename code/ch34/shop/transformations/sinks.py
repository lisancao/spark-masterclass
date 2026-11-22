"""Sink: send each shipped order to a JSON feed outside the pipeline, one time."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

spark: SparkSession

dp.create_sink(name="shipping_feed", format="json", options={"path": "exports/shipping_feed"})


@dp.append_flow(target="shipping_feed")
def shipped_orders_to_feed() -> DataFrame:
    return (spark.readStream.table("bronze_orders").where("status = 'shipped'")
            .select("order_id", "customer_id", "product_id", "quantity", "order_ts"))
