"""A streaming table with an explicit append flow, a temporary view and a materialized view."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ORDER_SCHEMA = ("order_id STRING, customer_id STRING, product_id STRING, quantity INT, "
                "unit_price STRING, status STRING, order_ts STRING")

dp.create_streaming_table("orders_raw", comment="Every order event, as it arrived")


@dp.append_flow(target="orders_raw")
def ingest_orders() -> DataFrame:
    return spark.readStream.schema(ORDER_SCHEMA).json("data/orders")


@dp.temporary_view
def placed_orders() -> DataFrame:
    return spark.table("orders_raw").where(F.col("status") == "placed")


@dp.materialized_view
def orders_by_status() -> DataFrame:
    return spark.table("orders_raw").groupBy("status").agg(F.count("*").alias("order_count"))
