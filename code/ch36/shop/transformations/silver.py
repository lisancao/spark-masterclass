"""Silver: typed, one row per order, every bronze order in silver or in quarantine."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark: SparkSession

ORDER_COLUMNS = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "status", "order_ts"]


@dp.materialized_view(comment="One row per customer, with typed dates", table_properties={"quality": "silver"})
def silver_customers() -> DataFrame:
    return spark.table("bronze_customers").select(
        "customer_id", "name", "city", F.to_date("signup_date").alias("signup_date"))


@dp.temporary_view
def orders_typed() -> DataFrame:
    # The first copy of an order wins. A file that sends an order again changes nothing.
    first_seen = Window.partitionBy("order_id").orderBy("_ingested_at", "_source_file")
    return (spark.table("bronze_orders")
            .select("*", F.row_number().over(first_seen).alias("_rn"))
            .where("_rn = 1")
            .select("order_id", "customer_id", "product_id", "quantity", "status", "_source_file",
                    F.col("unit_price").try_cast("decimal(10,2)").alias("unit_price"),
                    F.try_to_timestamp("order_ts").alias("order_ts")))


@dp.temporary_view
def orders_checked() -> DataFrame:
    # One reason for each order, computed once. Silver and quarantine both read it.
    known = spark.table("silver_customers").select("customer_id", "city")
    return (spark.table("orders_typed").join(known, "customer_id", "left")
            .select("*", F.when(F.col("unit_price").isNull(), "invalid_price")
                          .when(F.col("quantity") <= 0, "invalid_quantity")
                          .when(F.col("city").isNull(), "unknown_customer")
                          .alias("_reason")))


@dp.materialized_view(comment="Valid orders, typed and unique", table_properties={"quality": "silver"})
def silver_orders() -> DataFrame:
    return (spark.table("orders_checked").where("_reason IS NULL")
            .select(*ORDER_COLUMNS, "city", "_source_file",
                    (F.col("quantity") * F.col("unit_price")).alias("line_total")))


@dp.materialized_view(comment="Orders that failed a rule, with the reason")
def quarantine_orders() -> DataFrame:
    return spark.table("orders_checked").where("_reason IS NOT NULL").select(*ORDER_COLUMNS, "_reason")
