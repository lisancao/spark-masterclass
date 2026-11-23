"""Chapter 93, Step 9: which of this chapter's MLlib parts work over Spark Connect in 4.3.0?

Each check fits or calls one thing on a small table, in a Classic session and in a Connect session.
Run from code/:  ch93/server.sh start && uv run python ch93/connect_matrix.py
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession, functions as F  # noqa: E402
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.clustering import KMeans, BisectingKMeans, GaussianMixture, LDA, PowerIterationClustering  # noqa: E402
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator, RankingEvaluator  # noqa: E402
from pyspark.ml.feature import (VectorAssembler, StandardScaler, MinMaxScaler, RobustScaler, Imputer,  # noqa: E402
                                Bucketizer, QuantileDiscretizer, TargetEncoder, PCA, RFormula, SQLTransformer,
                                UnivariateFeatureSelector, CountVectorizer)
from pyspark.ml.fpm import FPGrowth, PrefixSpan  # noqa: E402
from pyspark.ml.recommendation import ALS  # noqa: E402
from pyspark.ml.stat import Correlation, ChiSquareTest, Summarizer  # noqa: E402
from pyspark.ml.tuning import CrossValidator, TrainValidationSplit, ParamGridBuilder  # noqa: E402
from common.book import TRIPS  # noqa: E402

URL = os.environ.get("CONNECT_URL", "sc://localhost:15118")


def table(spark):
    t = (spark.read.parquet(str(TRIPS)).where("fare_amount BETWEEN 0.01 AND 500 AND trip_distance BETWEEN 0.1 AND 100")
         .sample(fraction=0.01, seed=7)
         .select(F.col("PULocationID").alias("pu"), F.col("DOLocationID").alias("do"), "trip_distance",
                 "fare_amount", F.col("passenger_count").cast("double").alias("pc"),
                 F.col("payment_type").cast("double").alias("label"),
                 F.hour("tpep_pickup_datetime").alias("hour")))
    return VectorAssembler(inputCols=["trip_distance", "fare_amount"], outputCol="features").transform(t).cache()


def checks(spark):
    t = table(spark)
    pairs = t.groupBy("pu", "do").agg(F.count("*").cast("float").alias("n"))
    baskets = t.select(F.array_distinct(F.array(F.concat(F.lit("pu"), "pu"), F.concat(F.lit("do"), "do"))).alias("items"))
    seqs = (t.groupBy("pu").agg(F.collect_list(F.array(F.col("hour").cast("string"))).alias("sequence")))
    docs = baskets.select(F.col("items").alias("words"))
    grid_als = ALS(userCol="pu", itemCol="do", ratingCol="n", maxIter=3, coldStartStrategy="drop")
    grid = ParamGridBuilder().addGrid(grid_als.rank, [2, 4]).build()
    ev = RegressionEvaluator(labelCol="n")
    edges = pairs.select(F.col("pu").cast("long").alias("src"), F.col("do").cast("long").alias("dst"),
                         F.col("n").cast("double").alias("weight"))
    return {
        "KMeans": lambda: KMeans(k=3, seed=1).fit(t).transform(t).count(),
        "BisectingKMeans": lambda: BisectingKMeans(k=3, seed=1).fit(t).transform(t).count(),
        "GaussianMixture": lambda: GaussianMixture(k=3, seed=1).fit(t).transform(t).count(),
        "LDA": lambda: LDA(k=3, maxIter=2).fit(CountVectorizer(inputCol="words", outputCol="features").fit(docs)
                                                .transform(docs)).describeTopics().count(),
        "PowerIterationClustering": lambda: PowerIterationClustering(k=3, maxIter=5).assignClusters(edges).count(),
        "ClusteringEvaluator": lambda: ClusteringEvaluator().evaluate(KMeans(k=3, seed=1).fit(t).transform(t)),
        "ALS explicit": lambda: ALS(userCol="pu", itemCol="do", ratingCol="n", maxIter=3).fit(pairs).transform(pairs).count(),
        "ALS implicit + recommendForAllUsers": lambda: ALS(userCol="pu", itemCol="do", ratingCol="n", implicitPrefs=True,
                                                         maxIter=3).fit(pairs).recommendForAllUsers(5).count(),
        "RankingEvaluator": lambda: RankingEvaluator(k=2).evaluate(spark.createDataFrame(
            [([1.0, 2.0], [1.0, 3.0])], "prediction array<double>, label array<double>")),
        "FPGrowth": lambda: FPGrowth(minSupport=0.01).fit(baskets).associationRules.count(),
        "PrefixSpan": lambda: PrefixSpan(minSupport=0.5, maxPatternLength=2).findFrequentSequentialPatterns(seqs).count(),
        "Correlation": lambda: Correlation.corr(t, "features").count(),
        "ChiSquareTest": lambda: ChiSquareTest.test(t, "features", "label").count(),
        "Summarizer": lambda: t.select(Summarizer.metrics("mean", "max").summary(F.col("features"))).count(),
        "Imputer": lambda: Imputer(inputCols=["pc"], outputCols=["pc2"]).fit(t).transform(t).count(),
        "StandardScaler / MinMaxScaler / RobustScaler": lambda: [s(inputCol="features", outputCol="s").fit(t).transform(t).count()
                                                                for s in (StandardScaler, MinMaxScaler, RobustScaler)],
        "Bucketizer / QuantileDiscretizer": lambda: QuantileDiscretizer(numBuckets=4, inputCol="fare_amount", outputCol="q").fit(
            Bucketizer(splits=[0, 10, float("inf")], inputCol="fare_amount", outputCol="b").transform(t)).transform(t).count(),
        "TargetEncoder": lambda: TargetEncoder(inputCols=["pu"], outputCols=["pu_te"], labelCol="fare_amount",
                                               targetType="continuous").fit(t.withColumn("pu", F.col("pu").cast("double")))
                                               .transform(t.withColumn("pu", F.col("pu").cast("double"))).count(),
        "PCA": lambda: PCA(k=1, inputCol="features", outputCol="p").fit(t).transform(t).count(),
        "RFormula + SQLTransformer": lambda: Pipeline(stages=[SQLTransformer(statement="SELECT *, hour % 2 AS odd FROM __THIS__"),
                                                              RFormula(formula="fare_amount ~ trip_distance + odd", featuresCol="rf")]).fit(t)
                                                              .transform(t).count(),
        "UnivariateFeatureSelector": lambda: UnivariateFeatureSelector(featuresCol="features", labelCol="pc", outputCol="sel",
            selectionMode="numTopFeatures").setFeatureType("continuous").setLabelType("continuous")
            .setSelectionThreshold(1).fit(t.na.drop(subset=["pc"])).transform(t.na.drop(subset=["pc"])).count(),
        "CrossValidator": lambda: CrossValidator(estimator=grid_als, estimatorParamMaps=grid, evaluator=ev, numFolds=2)
                                  .fit(pairs).avgMetrics,
        "KMeansModel.summary.trainingCost": lambda: KMeans(k=3, seed=1).fit(t).summary.trainingCost,
        "GaussianMixtureModel.gaussiansDF": lambda: GaussianMixture(k=2, seed=1).fit(t).gaussiansDF.count(),
        "ALSModel.userFactors": lambda: ALS(userCol="pu", itemCol="do", ratingCol="n", maxIter=2).fit(pairs).userFactors.count(),
        "FPGrowthModel.freqItemsets": lambda: FPGrowth(minSupport=0.01).fit(baskets).freqItemsets.count(),
        "CrossValidator(collectSubModels=True)": lambda: len(CrossValidator(estimator=grid_als, estimatorParamMaps=grid,
            evaluator=ev, numFolds=2, collectSubModels=True).fit(pairs).subModels),
        "TrainValidationSplit": lambda: TrainValidationSplit(estimator=grid_als, estimatorParamMaps=grid,
                                                             evaluator=ev).fit(pairs).validationMetrics,
    }


def run(spark):
    out = {}
    for name, f in checks(spark).items():
        t0 = time.time()
        try:
            f()
            out[name] = "ok"
        except Exception as e:
            out[name] = f"FAILED  {type(e).__name__}: {' '.join(str(e).split())[:60]}"
    return out


if __name__ == "__main__":
    classic = SparkSession.builder.master("local[4]").config("spark.ui.enabled", "false").getOrCreate()
    classic.sparkContext.setLogLevel("OFF")
    a = run(classic)
    classic.stop()
    with SparkSession.builder.remote(URL).getOrCreate() as connect:
        b = run(connect)
    print(f"{'pyspark 4.3.0':<46} {'Classic':<10} Connect")
    for k in a:
        print(f"{k:<46} {a[k][:40]:<10} {b[k]}")
