"""Bronze: every input row as it arrived, with where it came from."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark: SparkSession

# Prices and times stay STRING here: under ANSI mode a bad value in a cast fails the run.
ORDER_SCHEMA = ("order_id STRING, customer_id STRING, product_id STRING, quantity INT, "
                "unit_price STRING, status STRING, order_ts STRING")
CUSTOMER_SCHEMA = "customer_id STRING, name STRING, email STRING, city STRING, signup_date STRING"
PRODUCT_SCHEMA = "product_id STRING, name STRING, category STRING, list_price DECIMAL(10,2)"


@dp.table(comment="Raw order events, append-only", table_properties={"quality": "bronze"})
def bronze_orders() -> DataFrame:
    return (spark.readStream.schema(ORDER_SCHEMA).json("data/orders")
            .select("*",
                    F.col("_metadata.file_name").alias("_source_file"),
                    F.col("_metadata.file_modification_time").alias("_file_mtime"),
                    F.current_timestamp().alias("_ingested_at")))


@dp.materialized_view(comment="The customer file as received", table_properties={"quality": "bronze"})
def bronze_customers() -> DataFrame:
    return spark.read.option("header", True).schema(CUSTOMER_SCHEMA).csv("data/customers.csv")


@dp.materialized_view(comment="The product file as received", table_properties={"quality": "bronze"})
def bronze_products() -> DataFrame:
    return spark.read.option("header", True).schema(PRODUCT_SCHEMA).csv("data/products.csv")
