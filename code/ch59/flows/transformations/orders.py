"""One streaming table, two append flows: new orders and a backfill of older orders."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark: SparkSession   # SDP puts `spark` into this module before it runs the file

ORDER_SCHEMA = ("order_id STRING, customer_id STRING, product_id STRING, quantity INT, "
                "unit_price STRING, status STRING, order_ts STRING")

dp.create_streaming_table("orders_raw", comment="Every order event, as it arrived")


@dp.append_flow(target="orders_raw")
def ingest_orders() -> DataFrame:
    return spark.readStream.schema(ORDER_SCHEMA).json("data/orders")


@dp.append_flow(target="orders_raw")
def backfill_orders() -> DataFrame:
    return spark.readStream.schema(ORDER_SCHEMA).json("data/backfill")
