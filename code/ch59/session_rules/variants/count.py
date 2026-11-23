from pyspark import pipelines as dp

@dp.materialized_view
def product_count():
    n = spark.table("products").count()     # an action inside a query function
    return spark.range(1).selectExpr(f"{n} AS products")
