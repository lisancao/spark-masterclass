"""Chapter 53: estimators, transformers and what fit() runs on the cluster.

Classic only: it labels jobs through spark.sparkContext to count them in the REST API.
Run from code/:  uv run python ch53/estimators.py
"""
import contextlib, fcntl, os, sys, time
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.ml import Estimator, Transformer
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F
from common.book import session, ui_api, TRIPS, MONTH_START, MONTH_END
from ch53.fares import fare_table, LABEL

@contextlib.contextmanager
def quiet_machine():
    """Hold the book's benchmark lock while timing, so other runs do not share the CPU."""
    with open(Path(os.environ.get("BOOK_DATA", "data")) / ".bench.lock", "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield


spark = session("ch53-estimators", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
data = fare_table(spark).cache()
print(f"training table: {data.count():,} rows")

# 1. An estimator's fit() returns a transformer.
si = StringIndexer(inputCol="rate_code", outputCol="rate_idx")
si_model = si.fit(data)
print("\n== estimator and transformer")
print(f"StringIndexer is an Estimator: {isinstance(si, Estimator)}, "
      f"a Transformer: {isinstance(si, Transformer)}")
print(f"fit() returned {type(si_model).__name__}, a Transformer: {isinstance(si_model, Transformer)}")

# 2. StringIndexer orders labels by frequency, and handleInvalid decides about NULL and unseen values.
print("\n== StringIndexer: labels by frequency")
counts = {r.rate_code: r["count"] for r in data.groupBy("rate_code").count().collect()}
for i, lab in enumerate(si_model.labels):
    print(f"  index {i}  rate_code {lab:<4} {counts[lab]:>8,} rows")
print(f"  NULL rate_code: {counts[None]:,} rows, not in labels")
probe = spark.createDataFrame([("1",), ("2",), ("7",), (None,)], "rate_code string")
for mode in ("error", "skip", "keep"):
    m = si.copy().setHandleInvalid(mode).fit(data)
    try:
        rows = [(r.rate_code, r.rate_idx) for r in m.transform(probe).collect()]
        print(f"  handleInvalid={mode:<5} -> {rows}")
    except Exception as e:
        msg = str(e)
        i = msg.find("Unseen label")
        j = msg.find("StringIndexer encountered NULL")
        print(f"  handleInvalid={mode:<5} -> error: {msg[max(i, j):][:60]}")

# 3. OneHotEncoder makes one slot per category, minus one (dropLast).
print("\n== OneHotEncoder")
idx = si.copy().setHandleInvalid("keep").fit(data).transform(data)
for drop in (True, False):
    ohe = OneHotEncoder(inputCol="rate_idx", outputCol="rate_vec", dropLast=drop).fit(idx)
    first = ohe.transform(idx.where("rate_code = '2'")).select("rate_vec").first()[0]
    print(f"  dropLast={drop!s:<5} size {first.size}, rate_code '2' -> {first}")

# 4. Feature importances are per slot of the assembled vector, not per input column.
print("\n== slots of an assembled vector")
va = VectorAssembler(inputCols=["trip_distance", "rate_vec"], outputCol="features")
enc = OneHotEncoder(inputCol="rate_idx", outputCol="rate_vec").fit(idx)
assembled = va.transform(enc.transform(idx))
attrs = assembled.schema["features"].metadata["ml_attr"]["attrs"]
slots = sorted((a["idx"], a["name"]) for kind in attrs.values() for a in kind)
print(f"  2 input columns -> {len(slots)} slots:", [n for _, n in slots])

# 5. What LinearRegression.fit() runs: one job per optimizer iteration.
def jobs_of(desc):
    port = sc.uiWebUrl.rsplit(":", 1)[1]          # the port Spark really bound
    js = [j for j in ui_api("jobs?status=succeeded", port=port) if j.get("description") == desc]
    return js, Counter(j["name"].split(" at ")[0] for j in js)

print("\n== what fit() runs")
feats = VectorAssembler(inputCols=["trip_distance", "hour", "weekday"], outputCol="features").transform(data)
for name, est in (("LinearRegression normal", LinearRegression(labelCol=LABEL, solver="normal")),
                  ("LinearRegression l-bfgs", LinearRegression(labelCol=LABEL, solver="l-bfgs",
                                                               regParam=0.1, elasticNetParam=0.5))):
    sc.setJobDescription(name)
    t0 = time.time()
    m = est.fit(feats)
    secs = time.time() - t0
    sc.setJobDescription(None)
    js, kinds = jobs_of(name)
    print(f"  {name:<24} {len(js):>3} jobs in {secs:.1f} s, iterations {m.summary.totalIterations:>3}, "
          f"by kind {dict(kinds.most_common(3))}")

# 6. Cached or not: the l-bfgs fit reads its input once per iteration.
print("\n== the same fit, input cached or not (median of 3 after a warmup)")
uncached = VectorAssembler(inputCols=["trip_distance", "hour", "weekday"], outputCol="features").transform(
    fare_table(spark))
lr = LinearRegression(labelCol=LABEL, solver="l-bfgs", regParam=0.1, elasticNetParam=0.5)
for label, df in (("cached", feats), ("not cached", uncached)):
    lr.fit(df)
    times = []
    with quiet_machine():
        for _ in range(3):
            t0 = time.time(); lr.fit(df); times.append(time.time() - t0)
    print(f"  {label:<11} median {sorted(times)[1]:.2f} s  {[round(t, 2) for t in times]}")

# 7. Classification: will a card trip have a tip?
print("\n== classification: tip or no tip (card trips)")
trips = spark.read.parquet(str(TRIPS))
card = (trips.where((F.col("tpep_pickup_datetime") >= MONTH_START) & (F.col("tpep_pickup_datetime") < MONTH_END)
                    & (F.col("payment_type") == 1) & F.col("fare_amount").between(0.01, 500)
                    & F.col("trip_distance").between(0.1, 100))
        .sample(fraction=0.05, seed=7)
        .select("trip_distance", "fare_amount",
                F.hour("tpep_pickup_datetime").cast("double").alias("hour"),
                F.col("PULocationID").cast("double").alias("pickup_zone"),
                (F.col("tip_amount") > 0).cast("double").alias("tipped")).cache())
ctrain, ctest = card.randomSplit([0.8, 0.2], seed=42)
rate = card.agg(F.avg("tipped")).first()[0]
print(f"  {card.count():,} card trips, {rate:.1%} with a tip")
cfeat = VectorAssembler(inputCols=["trip_distance", "fare_amount", "hour", "pickup_zone"], outputCol="features")
auc = BinaryClassificationEvaluator(labelCol="tipped", metricName="areaUnderROC")
acc = MulticlassClassificationEvaluator(labelCol="tipped", metricName="accuracy")
for est in (LogisticRegression(labelCol="tipped"),
            RandomForestClassifier(labelCol="tipped", numTrees=20, maxDepth=8, seed=42)):
    m = est.fit(cfeat.transform(ctrain))
    p = m.transform(cfeat.transform(ctest))
    print(f"  {type(est).__name__:<24} AUC {auc.evaluate(p):.3f}  accuracy {acc.evaluate(p):.3f}")
print("  columns a classifier adds:", [c for c in p.columns if c in ("rawPrediction", "probability", "prediction")])

# 8. New in 4.3: pruneTree on tree classifiers.
print("\n== pruneTree (new in 4.3)")
for prune in (True, False):
    rf = RandomForestClassifier(labelCol="tipped", numTrees=20, maxDepth=8, seed=42, pruneTree=prune)
    m = rf.fit(cfeat.transform(ctrain))
    nodes = sum(t.numNodes for t in m.trees)
    print(f"  pruneTree={prune!s:<5} {nodes:>6,} nodes in 20 trees, AUC {auc.evaluate(m.transform(cfeat.transform(ctest))):.3f}")

# 9. CrossValidator: how many fits?
fits = Counter()
class CountedLR(LinearRegression):
    def _fit(self, dataset):
        fits[(self.getRegParam(), self.getElasticNetParam())] += 1
        return super()._fit(dataset)

small = feats.sample(fraction=0.1, seed=1)
print("\n== CrossValidator: 4 settings x 3 folds")
for where in ("class attribute", "instance"):
    fits.clear()
    lr = CountedLR(labelCol=LABEL)
    owner = LinearRegression if where == "class attribute" else lr
    grid = (ParamGridBuilder().addGrid(owner.regParam, [0.0, 1.0])
            .addGrid(owner.elasticNetParam, [0.0, 1.0]).build())
    cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, numFolds=3,
                        evaluator=RegressionEvaluator(labelCol=LABEL), parallelism=4, seed=42)
    cvm = cv.fit(small)
    print(f"  grid keys from the {where}: {sum(fits.values())} fits, "
          f"settings seen by _fit {sorted(fits)}")
    print(f"     avgMetrics (rmse): {[round(float(x), 3) for x in cvm.avgMetrics]}")
