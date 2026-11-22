"""Chapter 35: the same fare pipeline over Spark Connect, and what the client actually holds.

Start the server first:  ch35/server.sh start
Run from code/:          uv run python ch35/connect_ml.py
"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession, functions as F
from common.book import DATA
from ch35.fares import fare_table, FEATURES_NUMERIC, FEATURES_CATEGORICAL, LABEL

url = os.environ.get("CONNECT_URL", "sc://localhost:10270")
with SparkSession.builder.remote(url).getOrCreate() as spark:
    print(type(spark).__module__, "| server runs Spark", spark.version)
    data = fare_table(spark)
    train, test = data.randomSplit([0.8, 0.2], seed=42)

    indexers = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in FEATURES_CATEGORICAL]
    pipe = Pipeline(stages=indexers + [
        VectorAssembler(inputCols=FEATURES_NUMERIC + [c + "_idx" for c in FEATURES_CATEGORICAL], outputCol="features"),
        GBTRegressor(featuresCol="features", labelCol=LABEL, maxIter=30, maxDepth=6, maxBins=300, seed=42)])

    t0 = time.time()
    model = pipe.fit(train)                     # every estimator is fitted on the server
    print("\nfit() returned once the server finished the fit")

    # What the client holds: references, not weights.
    print("\nstages in the client's PipelineModel:")
    for s in model.stages:
        print(f"  {type(s).__name__:<22} _java_obj = {s._java_obj!s:.60}")

    # The server's ML cache for this session: one entry per fitted model, with its estimated size.
    print("\nserver ML cache for this session:")
    from collections import defaultdict
    by_class = defaultdict(list)
    for item in spark.client._get_ml_cache_info():
        e = json.loads(item)
        by_class[e["class"].rsplit(".", 1)[-1]].append(e["size"])
    for cls, sizes in sorted(by_class.items(), key=lambda kv: -sum(kv[1])):
        print(f"  {cls:<28} {len(sizes):>3} entries {sum(sizes):>12,} bytes")

    # Attributes are fetched from the server on access.
    gbt = model.stages[-1]
    t0 = time.time()
    trees = gbt.getNumTrees
    imp = gbt.featureImportances
    print(f"\ngetNumTrees = {trees}, featureImportances has {imp.size} entries"
          " (each attribute is one round trip to the server)")

    # transform() adds a plan node that names the cached model; nothing runs yet.
    scored = model.transform(test)
    rel = scored._plan.to_proto(spark.client).root
    node, depth = rel, 0
    print("\nthe plan the client sends for model.transform(test), outermost nodes:")
    while depth < 6:
        kind = node.WhichOneof("rel_type")
        detail = ""
        if kind == "ml_relation":
            t = node.ml_relation.transform
            detail = (f"obj_ref {t.obj_ref.id[:8]}..." if t.HasField("obj_ref")
                      else f"transformer {t.transformer.name}")
            node = t.input
        else:
            print(f"  {'  ' * depth}{kind}")
            break
        print(f"  {'  ' * depth}{kind}: {detail}")
        depth += 1

    ev = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction", metricName="rmse")
    print(f"\ntest rmse {ev.evaluate(scored):.2f}  (evaluated on the server as well)")

    # save() writes on the server's file system, not the client's.
    path = str(DATA / "ch35" / "fare_gbt_connect")
    model.write().overwrite().save(path)
    print("saved by the server to", path.replace(os.environ["HOME"], "~"))

    # The RDD-based API is not there.
    try:
        from pyspark.mllib.regression import LabeledPoint  # noqa: F401
        spark.sparkContext
    except Exception as e:
        print("\npyspark.mllib needs a SparkContext:", type(e).__name__, str(e).splitlines()[0][:70])

