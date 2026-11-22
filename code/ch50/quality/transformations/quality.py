"""Quality without expectations: rules as data, a per-file report, a gate and a quarantine."""
import json

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark: SparkSession

RULES = json.load(open("data/rules.json"))   # module scope: read once, at registration
MAX_ERROR_RATIO = 0.2
ORDER_SCHEMA = ("order_id STRING, customer_id STRING, order_ts STRING, quantity STRING, "
                "unit_price STRING, status STRING, currency STRING")


def failed(severity: str):
    """The names of the rules of one severity that a row breaks. A NULL check counts as broken."""
    return F.array_compact(F.array(*[
        F.when(~F.coalesce(F.expr(r["check"]), F.lit(False)), F.lit(r["name"]))
        for r in RULES if r["severity"] == severity]))


@dp.table(comment="Every order row as received, all columns as text")
def bronze_orders() -> DataFrame:
    return (spark.readStream.option("header", True).schema(ORDER_SCHEMA).csv("data/orders")
            .select("*", F.col("_metadata.file_name").alias("_source_file"))
            .select("*", F.to_date(F.regexp_extract("_source_file", r"(\d{4}-\d{2}-\d{2})", 1))
                    .alias("_file_date")))


@dp.materialized_view(comment="One row per customer: the first row in the file wins")
def customers_dim() -> DataFrame:
    first = Window.partitionBy("customer_id").orderBy("_row")
    return (spark.read.option("header", True).csv("data/customers.csv")
            .select("*", F.monotonically_increasing_id().alias("_row"))
            .select("*", F.row_number().over(first).alias("_rn")).where("_rn = 1")
            .select("customer_id", "name", "country"))


@dp.temporary_view
def orders_checked() -> DataFrame:
    known = spark.table("customers_dim").select("customer_id", F.lit(True).alias("_in_dim"))
    return (spark.table("bronze_orders").join(known, "customer_id", "left")
            .select("*", F.coalesce("_in_dim", F.lit(False)).alias("_customer_known"))
            .select("*", failed("error").alias("failed_rules"), failed("warn").alias("warnings")))


@dp.materialized_view(comment="Quality for each input file")
def quality_report() -> DataFrame:
    return (spark.table("orders_checked").groupBy("_source_file")
            .agg(F.count("*").alias("rows"),
                 F.count_if(F.size("failed_rules") > 0).alias("quarantined"),
                 F.count_if(F.size("warnings") > 0).alias("warned"))
            .select("*", F.round(F.col("quarantined") / F.col("rows"), 4).alias("error_ratio")))


@dp.temporary_view
def quality_gate() -> DataFrame:
    return spark.table("quality_report").agg(
        F.assert_true(F.max("error_ratio") <= MAX_ERROR_RATIO,
                      f"quality gate: a file has more than {MAX_ERROR_RATIO:.0%} bad rows")
        .alias("_gate"))


@dp.materialized_view(comment="Valid orders, typed, one row per order_id")
def silver_orders() -> DataFrame:
    first_seen = Window.partitionBy("order_id").orderBy("_file_date")
    return (spark.table("orders_checked")
            .crossJoin(spark.table("quality_gate")).where(F.col("_gate").isNull())  # the gate
            .where(F.size("failed_rules") == 0)
            .select("*", F.row_number().over(first_seen).alias("_rn")).where("_rn = 1")
            .select("order_id", "customer_id", F.try_to_timestamp("order_ts").alias("order_ts"),
                    F.col("quantity").cast("int").alias("quantity"),
                    F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
                    F.lower(F.trim("status")).alias("status"), "currency", "warnings",
                    "_source_file"))


@dp.materialized_view(comment="Rows that broke at least one rule, with the rule names")
def quarantine_orders() -> DataFrame:
    return (spark.table("orders_checked").where(F.size("failed_rules") > 0)
            .select("order_id", "customer_id", "quantity", "unit_price", "status", "currency",
                    "failed_rules", "_source_file"))


@dp.materialized_view(comment="Gold: silver revenue per currency")
def revenue_by_currency() -> DataFrame:
    return (spark.table("silver_orders").groupBy("currency")
            .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),
                 F.count("*").alias("orders")))
