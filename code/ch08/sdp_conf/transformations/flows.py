from pyspark import pipelines as dp
from pyspark.sql import functions as F

print("module scope: spark.sql.shuffle.partitions =",
      spark.conf.get("spark.sql.shuffle.partitions"))


@dp.materialized_view
def by_key_spec():
    """Uses the configuration of the spec."""
    return spark.range(1000).groupBy((F.col("id") % 10).alias("k")).count()


@dp.materialized_view(spark_conf={"spark.sql.shuffle.partitions": "5"})
def by_key_flow():
    """Sets its own value for this flow only."""
    return spark.range(1000).groupBy((F.col("id") % 10).alias("k")).count()
