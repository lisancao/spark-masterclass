"""Chapter 35: track the fare models in open source MLflow (a local SQLite store, no server).

Run from code/:  uv run --with mlflow==3.18.0 python ch35/mlflow_tracking.py
"""
import os, shutil, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")

import mlflow
import mlflow.spark
from mlflow.models import infer_signature
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from common.book import session, DATA
from ch35.fares import fare_table, FEATURES_NUMERIC, FEATURES_CATEGORICAL, LABEL

store = DATA / "ch35" / "mlflow"
shutil.rmtree(store, ignore_errors=True)
store.mkdir(parents=True)
mlflow.set_tracking_uri(f"sqlite:///{store}/mlflow.db")
exp = mlflow.create_experiment("taxi-fares", artifact_location=(store / "artifacts").as_uri())
mlflow.set_experiment(experiment_id=exp)
print("mlflow", mlflow.__version__, "| tracking store:", mlflow.get_tracking_uri().replace(os.environ["HOME"], "~"))

spark = session("ch35-mlflow", spark_ui_showConsoleProgress="false")
data = fare_table(spark).cache()
train, test = data.randomSplit([0.8, 0.2], seed=42)
ev = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction")

idx = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in FEATURES_CATEGORICAL]
def linear():
    return Pipeline(stages=idx + [
        OneHotEncoder(inputCols=[c + "_idx" for c in FEATURES_CATEGORICAL], outputCols=[c + "_vec" for c in FEATURES_CATEGORICAL]),
        VectorAssembler(inputCols=FEATURES_NUMERIC + [c + "_vec" for c in FEATURES_CATEGORICAL], outputCol="features"),
        LinearRegression(labelCol=LABEL)])
def boosted(depth, iters):
    return Pipeline(stages=idx + [
        VectorAssembler(inputCols=FEATURES_NUMERIC + [c + "_idx" for c in FEATURES_CATEGORICAL], outputCol="features"),
        GBTRegressor(labelCol=LABEL, maxDepth=depth, maxIter=iters, maxBins=300, seed=42)])

candidates = [("linear", {}, linear()),
              ("gbt", {"maxDepth": 4, "maxIter": 40}, boosted(4, 40)),
              ("gbt", {"maxDepth": 6, "maxIter": 30}, boosted(6, 30))]
example = test.select(*FEATURES_NUMERIC, *FEATURES_CATEGORICAL).limit(5).toPandas()

for kind, params, pipe in candidates:
    with mlflow.start_run(run_name=f"{kind} {params}".strip()) as run:
        mlflow.log_params({"model": kind, "sample_fraction": 0.1, "train_rows": train.count(), **params})
        mlflow.set_tag("spark_version", spark.version)
        t0 = time.time()
        model = pipe.fit(train)
        mlflow.log_metric("fit_seconds", time.time() - t0)
        preds = model.transform(test)
        for m in ("rmse", "mae", "r2"):
            mlflow.log_metric(m, ev.setMetricName(m).evaluate(preds))
        signature = infer_signature(example, model.transform(spark.createDataFrame(example)).select("prediction").toPandas())
        mlflow.spark.log_model(model, "model", signature=signature, input_example=example)
        print(f"logged run {run.info.run_id[:8]}  {kind:<7}{params}")

# Compare runs from the store, not from memory.
runs = mlflow.search_runs(experiment_ids=[exp], order_by=["metrics.rmse ASC"])
cols = ["run_id", "params.model", "params.maxDepth", "params.maxIter", "metrics.rmse", "metrics.mae", "metrics.r2"]
table = runs[cols].copy()
table["run_id"] = table["run_id"].str[:8]
print("\n" + table.round(3).to_string(index=False))

# What a logged Spark model is on disk.
best = runs.iloc[0]
root = Path(mlflow.artifacts.download_artifacts(run_id=best.run_id, artifact_path="model"))
print(f"\nartifacts of the best run ({best.run_id[:8]}):")
for p in sorted(root.rglob("*")):
    rel = p.relative_to(root)
    if len(rel.parts) <= 2 or (len(rel.parts) == 3 and rel.parts[-1] in ("metadata", "data")):
        print("  ", str(rel) + ("/" if p.is_dir() else ""))
from mlflow.models import Model
mlmodel = Model.load(str(root / "MLmodel"))
print("\nMLmodel flavors:", list(mlmodel.flavors), "| spark flavor:",
      {k: v for k, v in mlmodel.flavors["spark"].items() if k != "code"})
print("signature inputs:", [(c.name, str(c.type)) for c in mlmodel.signature.inputs.inputs])

# Load it back two ways.
uri = f"runs:/{best.run_id}/model"
pm = mlflow.spark.load_model(uri)
print("mlflow.spark.load_model ->", type(pm).__name__, "with", len(pm.stages), "stages")
pyfunc = mlflow.pyfunc.load_model(uri)
print("mlflow.pyfunc.load_model(...).predict(5 rows) ->", [round(x, 2) for x in pyfunc.predict(example)])
