"""Two materialized views over a small CSV file: the products, and a summary by category."""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PRODUCT_SCHEMA = "product_id STRING, name STRING, category STRING, list_price DECIMAL(10,2)"


@dp.materialized_view(comment="The product list, read in full on every run")
def products() -> DataFrame:
    return spark.read.option("header", True).schema(PRODUCT_SCHEMA).csv("data/products.csv")


@dp.materialized_view
def products_by_category() -> DataFrame:
    print("query function products_by_category: called")   # shows when SDP calls it
    return (spark.table("products").groupBy("category")
            .agg(F.count("*").alias("products"), F.avg("list_price").alias("avg_list_price")))
