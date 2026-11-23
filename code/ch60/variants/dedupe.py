"""Variant of orders_typed: dropDuplicates and drop on a pipeline dataset."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view
def orders_typed():
    return (spark.table("bronze_orders").dropDuplicates(["order_id"]).drop("_file_mtime")
            .select("order_id", "customer_id", "product_id", "quantity", "status", "_source_file",
                    F.col("unit_price").try_cast("decimal(10,2)").alias("unit_price"),
                    F.try_to_timestamp("order_ts").alias("order_ts")))
