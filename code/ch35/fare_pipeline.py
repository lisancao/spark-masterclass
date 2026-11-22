"""Chapter 35: an MLlib pipeline that predicts a taxi fare from what is known when the trip starts.

Run from code/:  uv run python ch35/fare_pipeline.py
"""
import shutil, sys, textwrap, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import functions as F
from common.book import session, plan, DATA
from ch35.fares import fare_table, FEATURES_NUMERIC, FEATURES_CATEGORICAL, LABEL

spark = session("ch35-fares", spark_ui_showConsoleProgress="false")
data = fare_table(spark).withColumn("trip_id", F.monotonically_increasing_id()).cache()
train, test = data.randomSplit([0.8, 0.2], seed=42)
print(f"rows: {data.count():,} (train {train.count():,}, test {test.count():,})")

# 1. VectorAssembler and a NULL feature.
try:
    VectorAssembler(inputCols=["trip_distance", "rate_code_raw"], outputCol="f").transform(train).agg(F.count("f")).first()
except Exception as e:
    msg = str(e)
    print("\nassembler over rate_code_raw ->")
    print(textwrap.fill(msg[msg.find("Encountered"):].splitlines()[0], 88, initial_indent="  ", subsequent_indent="  "))

# 2. Two pipelines over the same stages for the categorical columns.
indexers = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in FEATURES_CATEGORICAL]
onehot = OneHotEncoder(inputCols=[c + "_idx" for c in FEATURES_CATEGORICAL],
                       outputCols=[c + "_vec" for c in FEATURES_CATEGORICAL])
linear = Pipeline(stages=indexers + [onehot,
    VectorAssembler(inputCols=FEATURES_NUMERIC + [c + "_vec" for c in FEATURES_CATEGORICAL], outputCol="features"),
    LinearRegression(featuresCol="features", labelCol=LABEL)])
tree_features = VectorAssembler(inputCols=FEATURES_NUMERIC + [c + "_idx" for c in FEATURES_CATEGORICAL],
                                outputCol="features")
gbt = GBTRegressor(featuresCol="features", labelCol=LABEL, maxIter=30, maxDepth=6, seed=42)

# A tree over StringIndexer output sees nominal metadata, so maxBins must cover every zone.
try:
    Pipeline(stages=indexers + [tree_features, gbt]).fit(train)
except Exception as e:
    msg = str(e)
    print("\nGBT with default maxBins ->")
    print(textwrap.fill(msg[msg.find("DecisionTree requires"):].splitlines()[0], 88, initial_indent="  ", subsequent_indent="  "))

gbt.setMaxBins(300)
boosted = Pipeline(stages=indexers + [tree_features, gbt])

# 3. Fit both, evaluate on the held-out 20%.
ev = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction")
mean_fare = train.agg(F.avg(LABEL)).first()[0]
baseline = test.withColumn("prediction", F.lit(mean_fare))
print(f"\n{'model':<22}{'fit s':>7}{'rmse':>8}{'mae':>8}{'r2':>8}")
def report(name, preds, secs=0.0):
    m = {k: ev.setMetricName(k).evaluate(preds) for k in ("rmse", "mae", "r2")}
    print(f"{name:<22}{secs:>7.1f}{m['rmse']:>8.2f}{m['mae']:>8.2f}{m['r2']:>8.3f}")
report("mean of train", baseline)
models = {}
for name, pipe in (("linear regression", linear), ("gradient-boosted trees", boosted)):
    t0 = time.time()
    models[name] = pipe.fit(train)
    report(name, models[name].transform(test), time.time() - t0)

# 4. What the boosted model leans on: importances, mapped back through the assembler's metadata.
model = models["gradient-boosted trees"]
scored = model.transform(test)
attrs = scored.schema["features"].metadata["ml_attr"]["attrs"]
names = {a["idx"]: a["name"] for kind in attrs.values() for a in kind}
imp = model.stages[-1].featureImportances
print("\nfeature importances:")
for i in sorted(range(len(names)), key=lambda i: -imp[i]):
    print(f"  {names[i]:<18}{imp[i]:.3f}")

# The JFK flat fare (rate code 2) is the case a linear model on distance cannot see.
print("\nmean absolute error by rate code (test set):")
lin = models["linear regression"].transform(test).select("trip_id", F.col("prediction").alias("lin"))
(scored.select("trip_id", "rate_code", LABEL, "prediction").join(lin, "trip_id")
    .groupBy("rate_code").agg(F.count("*").alias("trips"),
                              F.round(F.avg(F.abs(F.col(LABEL) - F.col("lin"))), 2).alias("linear"),
                              F.round(F.avg(F.abs(F.col(LABEL) - F.col("prediction"))), 2).alias("gbt"))
    .orderBy(F.desc("trips")).show())

# 5. What a fitted pipeline runs: the physical plan of transform().
print(plan(model.transform(test.limit(5)).select("prediction"), "simple"))

# 6. Unseen categories: a zone that was never in the training data.
odd = spark.createDataFrame([(2.5, 9.0, 3.0, "999", "161", "1", None, 0.0, 0)], data.schema)
print("unseen zone '999' ->", model.transform(odd).select("pickup_zone_idx", F.round("prediction", 2)).first())

# 7. Save, list, load, and compare.
path = DATA / "ch35" / "fare_gbt"
shutil.rmtree(path, ignore_errors=True)
model.write().overwrite().save(str(path))
print("\nsaved model:")
for p in sorted(path.rglob("*")):
    if p.is_dir() and p.parent.name == "stages" or p.name in ("metadata", "data") and p.parent == path:
        print("  ", p.relative_to(path))
loaded = PipelineModel.load(str(path))
a = model.transform(test).select("prediction").collect()
b = loaded.transform(test).select("prediction").collect()
print("loaded model predicts the same:", a == b, f"({len(a):,} rows)")

# 8. Tuning: TrainValidationSplit fits each grid point once.
grid = ParamGridBuilder().addGrid(gbt.maxDepth, [4, 6]).addGrid(gbt.maxIter, [20, 40]).build()
tvs = TrainValidationSplit(estimator=boosted, estimatorParamMaps=grid, evaluator=ev.setMetricName("rmse"),
                           trainRatio=0.8, parallelism=2, seed=42)
t0 = time.time()
tuned = tvs.fit(train)
print(f"\nTrainValidationSplit: {len(grid)} settings, {time.time() - t0:.0f} s")
for params, metric in zip(grid, tuned.validationMetrics):
    print("  ", {p.name: v for p, v in params.items()}, f"validation rmse {metric:.3f}")
best = tuned.bestModel.stages[-1]
print("best:", {"maxDepth": best.getMaxDepth(), "maxIter": best.getMaxIter()},
      f"test rmse {ev.evaluate(tuned.bestModel.transform(test)):.3f}")
