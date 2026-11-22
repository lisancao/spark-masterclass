# Chapter 79: a pipeline file that prints what the pipeline session sees. Module code runs in the
# CLI process at registration, so the prints appear in the CLI output.
import os

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

conf = spark.conf
print("probe: spark.sql.shuffle.partitions   =", conf.get("spark.sql.shuffle.partitions"))
print("probe: spark.sql.adaptive.enabled     =", conf.get("spark.sql.adaptive.enabled"))
print("probe: taxi.min_fare                  =", conf.get("taxi.min_fare", "<not set>"))
print("probe: spark.sql.catalogImplementation =", conf.get("spark.sql.catalogImplementation"))
print("probe: current catalog and database   =", spark.catalog.currentCatalog(),
      spark.catalog.currentDatabase())
print("probe: TAXI_ENV in the CLI process    =", os.environ.get("TAXI_ENV", "<not set>"))


@dp.materialized_view(spark_conf={"spark.sql.shuffle.partitions": "2"})
def zones_by_borough() -> DataFrame:
    return spark.table("zones").groupBy("Borough").count()
