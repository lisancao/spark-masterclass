"""Gold: tables shaped by the questions that the business asks."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark: SparkSession

CATEGORIES = ["hardware", "merch"]   # add to this list, never remove from it


@dp.temporary_view
def billable_orders() -> DataFrame:
    """The one place that says which orders count as revenue."""
    return spark.table("silver_orders").where("status <> 'cancelled'")


@dp.materialized_view(comment="Revenue per day, with a running total", table_properties={"quality": "gold"})
def gold_daily_revenue() -> DataFrame:
    running = Window.orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    daily = (spark.table("billable_orders")
             .groupBy(F.to_date("order_ts").alias("order_date"))
             .agg(F.sum("line_total").alias("revenue"), F.count("*").alias("orders"),
                  F.round(F.avg("line_total"), 2).alias("avg_order_value")))
    return daily.select("*", F.sum("revenue").over(running).alias("cumulative_revenue"))


@dp.materialized_view(comment="Revenue per customer, ranked", table_properties={"quality": "gold"})
def gold_customer_value() -> DataFrame:
    by_revenue = Window.orderBy(F.col("revenue").desc())
    everyone = Window.partitionBy()
    per_customer = (spark.table("billable_orders").groupBy("customer_id", "city")
                    .agg(F.sum("line_total").alias("revenue"), F.count("*").alias("orders")))
    return per_customer.select(
        "*", F.dense_rank().over(by_revenue).alias("revenue_rank"),
        F.round(F.col("revenue") / F.sum("revenue").over(everyone), 4).alias("revenue_share"))


for category in CATEGORIES:
    @dp.materialized_view(name=f"gold_revenue_{category}", table_properties={"quality": "gold"})
    def revenue_for_category(category: str = category) -> DataFrame:
        products = (spark.table("bronze_products").where(F.col("category") == category)
                    .select("product_id", F.col("name").alias("product")))
        return (spark.table("billable_orders").join(products, "product_id")
                .groupBy("product_id", "product").agg(F.sum("line_total").alias("revenue")))
