from pyspark import pipelines as dp
print("module scope: runs once, when the CLI imports the file")

@dp.materialized_view
def products_by_category():
    print("query function: called")
    return spark.table("products").groupBy("category").count()
