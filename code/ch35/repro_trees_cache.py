"""Open finding (Part VII notes): on Spark Connect, Params.params evaluates the `trees`
cached_property of a tree-ensemble model, which fetches every tree into the server's ML cache."""
import gc, sys
from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler

with SparkSession.builder.remote(sys.argv[1] if len(sys.argv) > 1 else "sc://localhost:10270").getOrCreate() as spark:
    df = VectorAssembler(inputCols=["x"], outputCol="features").transform(
        spark.range(200).selectExpr("cast(id as double) x", "cast(id % 7 as double) label"))
    m = GBTRegressor(maxIter=5).fit(df)
    print("cache entries after fit:          ", len(spark.client._get_ml_cache_info()))
    m.params
    print("cache entries after model.params: ", len(spark.client._get_ml_cache_info()))
    del m; gc.collect()
    print("cache entries after del + gc:     ", len(spark.client._get_ml_cache_info()))
