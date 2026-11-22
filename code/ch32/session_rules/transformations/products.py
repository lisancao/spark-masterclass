from pyspark import pipelines as dp

@dp.materialized_view
def products():
    return spark.read.option("header", True).csv("data/products.csv")
