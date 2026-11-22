"""Variant of orders_typed: withColumn on a pipeline dataset (SPARK-59663 on 4.2.0)."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.temporary_view
def orders_typed():
    first_seen = Window.partitionBy("order_id").orderBy("_ingested_at", "_source_file")
    return (spark.table("bronze_orders")
            .withColumn("_rn", F.row_number().over(first_seen))
            .where("_rn = 1")
            .withColumn("unit_price", F.col("unit_price").try_cast("decimal(10,2)"))
            .withColumn("order_ts", F.try_to_timestamp("order_ts"))
            .select("order_id", "customer_id", "product_id", "quantity", "status", "_source_file",
                    "unit_price", "order_ts"))
