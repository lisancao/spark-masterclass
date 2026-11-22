from pyspark import pipelines as dp

ORDERS = "file:///tmp/ch49-pipeline/orders.ndjson"


@dp.materialized_view
def bronze_orders():
    return spark.read.text(ORDERS)
