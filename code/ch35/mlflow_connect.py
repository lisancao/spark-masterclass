"""Chapter 35: log a model fitted over Spark Connect with MLflow.

Start the server first:  ch35/server.sh start
Run from code/:          uv run --with mlflow==3.18.0 python ch35/mlflow_connect.py
"""
import os, sys, textwrap
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")

import mlflow, mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from common.book import DATA
from ch35.fares import fare_table, LABEL

store = DATA / "ch35" / "mlflow"
mlflow.set_tracking_uri(f"sqlite:///{store}/mlflow.db")
mlflow.set_experiment("taxi-fares")

with SparkSession.builder.remote(os.environ.get("CONNECT_URL", "sc://localhost:15057")).getOrCreate() as spark:
    train = fare_table(spark, fraction=0.01)
    model = Pipeline(stages=[StringIndexer(inputCol="rate_code", outputCol="rate_idx", handleInvalid="keep"),
                             VectorAssembler(inputCols=["trip_distance", "rate_idx"], outputCol="features"),
                             LinearRegression(labelCol=LABEL)]).fit(train)
    with mlflow.start_run(run_name="linear over connect"):
        try:
            info = mlflow.spark.log_model(model, "model")
            print("logged from a Connect session:", info.model_uri)
            back = mlflow.spark.load_model(info.model_uri)
            print("loaded back as", type(back).__module__ + "." + type(back).__name__)
        except Exception as e:
            print("log_model from a Connect session ->")
            print(textwrap.fill(f"{type(e).__name__}: {str(e).splitlines()[0]}", 88,
                                initial_indent="  ", subsequent_indent="  "))

        # Workaround: the server writes the model where both sides can read it, the client logs the files.
        shared = store / "connect_model"
        model.write().overwrite().save(str(shared))
        mlflow.log_artifacts(str(shared), artifact_path="sparkml")
        mlflow.log_param("saved_by", "server, then logged with mlflow.log_artifacts")
        files = sorted(p.name for p in (shared / "stages").iterdir())
        print("workaround: server save + log_artifacts ->", len(files), "stages logged:", files)
