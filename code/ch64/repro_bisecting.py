"""Chapter 64 open finding: BisectingKMeans does not split centered data, depending on the partitions.
Synthetic data: three well-separated groups of 100 points. Run from code/:  uv run python ch64/repro_bisecting.py"""
from pyspark.sql import SparkSession
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors
import random
spark = SparkSession.builder.master("local[4]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
random.seed(1)
rows = [(Vectors.dense([random.gauss(c, 0.3), random.gauss(-c, 0.3)]),) for i in range(300) for c in [(i % 3) * 5]]
df = StandardScaler(inputCol="raw", outputCol="features", withMean=True).fit(spark.createDataFrame(rows, ["raw"])) \
    .transform(spark.createDataFrame(rows, ["raw"]))
for n in (1, 2, 4, 8, 16, 32, 200):
    print(n, BisectingKMeans(k=3, seed=7).fit(df.repartition(n)).summary.clusterSizes)
