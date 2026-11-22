"""Checks: datasets that fail the run when two layers disagree."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark: SparkSession


def expect(df: DataFrame, condition: str, message: str) -> DataFrame:
    # Aggregate first, so the check always gives exactly one row, also for an empty input.
    return df.agg(F.expr(condition).alias("ok")).select(F.assert_true("ok", message).alias("ok"))


@dp.materialized_view
def check_orders_accounted_for() -> DataFrame:
    counts = (spark.table("bronze_orders").agg(F.countDistinct("order_id").alias("bronze"))
              .crossJoin(spark.table("silver_orders").agg(F.count("*").alias("silver")))
              .crossJoin(spark.table("quarantine_orders").agg(F.count("*").alias("quarantined"))))
    return expect(counts, "first(silver) + first(quarantined) = first(bronze)",
                  "each unique bronze order must be in silver_orders or in quarantine_orders")


@dp.materialized_view
def check_gold_reconciles() -> DataFrame:
    sums = (spark.table("billable_orders").agg(F.sum("line_total").alias("silver"))
            .crossJoin(spark.table("gold_daily_revenue").agg(F.sum("revenue").alias("daily")))
            .crossJoin(spark.table("gold_customer_value").agg(F.sum("revenue").alias("customers"))))
    return expect(sums, "first(daily) = first(silver) AND first(customers) = first(silver)",
                  "the gold tables must add up to the billable silver revenue")
