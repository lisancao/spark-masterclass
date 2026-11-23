"""Chapter 64, Step 7: feature transformers that Chapter 63 did not use.

Imputer, three scalers, Bucketizer and QuantileDiscretizer, TargetEncoder (4.0), PCA, RFormula,
SQLTransformer, UnivariateFeatureSelector. Run from code/:  uv run python ch64/features.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.feature import (Imputer, VectorAssembler, StandardScaler, MinMaxScaler, RobustScaler,  # noqa: E402
                                Bucketizer, QuantileDiscretizer, TargetEncoder, PCA, RFormula,
                                SQLTransformer, UnivariateFeatureSelector, OneHotEncoder, StringIndexer)
from pyspark.ml.functions import vector_to_array  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from common.book import session  # noqa: E402
from ch64.taxi93 import trips, zone_profiles, PROFILE_COLS  # noqa: E402

spark = session("ch64-features", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="16")
t = trips(spark).sample(fraction=0.05, seed=7).cache()
print(f"sample: {t.count():,} trips")

print("\n== Imputer: passenger_count is NULL for some trips")
n_null = t.where(F.col("passenger_count").isNull()).count()
for strategy in ("mean", "median", "mode"):
    m = Imputer(inputCols=["passenger_count"], outputCols=["pc"], strategy=strategy).fit(
        t.withColumn("passenger_count", F.col("passenger_count").cast("double")))
    print(f"  strategy={strategy:<6} fills {n_null:,} NULL values with {m.surrogateDF.first()[0]:.3f}")

print("\n== three scalers on fare_amount (5 rows: the 4 quartile points and the maximum)")
q = t.approxQuantile("fare_amount", [0.0, 0.25, 0.5, 0.75], 0.0) + [t.agg(F.max("fare_amount")).first()[0]]
pts = spark.createDataFrame([(float(x),) for x in q], "fare_amount double")
fv = VectorAssembler(inputCols=["fare_amount"], outputCol="f")
tv = fv.transform(t)
rows = {"fare": q}
for name, sc in (("StandardScaler", StandardScaler(inputCol="f", outputCol="s", withMean=True)),
                 ("MinMaxScaler", MinMaxScaler(inputCol="f", outputCol="s")),
                 ("RobustScaler", RobustScaler(inputCol="f", outputCol="s"))):
    out = sc.fit(tv).transform(fv.transform(pts)).select(vector_to_array("s")[0].alias("x")).collect()
    rows[name] = [r.x for r in out]
for k, v in rows.items():
    print(f"  {k:<15}" + " ".join(f"{x:9.3f}" for x in v))

print("\n== Bucketizer (fixed splits) and QuantileDiscretizer (splits from the data)")
b = Bucketizer(splits=[0, 10, 20, 30, 60, float("inf")], inputCol="fare_amount", outputCol="band")
qd = QuantileDiscretizer(numBuckets=5, inputCol="fare_amount", outputCol="qband", relativeError=0.001).fit(t)
print(f"  QuantileDiscretizer splits: {[round(x, 2) for x in qd.getSplits()]}")
both = qd.transform(b.transform(t))
print("  rows per band: Bucketizer", [r["count"] for r in both.groupBy("band").count().orderBy("band").collect()],
      "| QuantileDiscretizer", [r["count"] for r in both.groupBy("qband").count().orderBy("qband").collect()])

print("\n== TargetEncoder (new in 4.0): a zone becomes the mean tip percent of its trips")
card = t.where(F.col("tip_pct").isNotNull() & (F.col("tip_pct") <= 100)) \
        .withColumn("pu", F.col("PULocationID").cast("double"))
te = TargetEncoder(inputCols=["pu"], outputCols=["pu_te"], labelCol="tip_pct", targetType="continuous",
                   smoothing=0.0).fit(card)
te_s = TargetEncoder(inputCols=["pu"], outputCols=["pu_te"], labelCol="tip_pct", targetType="continuous",
                     smoothing=100.0).fit(card)
ohe = OneHotEncoder(inputCols=["pu"], outputCols=["pu_vec"]).fit(card)
print(f"  OneHotEncoder: one vector of size {ohe.categorySizes[0] - 1} per row | TargetEncoder: one double per row")
cnt = card.groupBy("pu").count()
rare = cnt.orderBy("count", "pu").first().pu
common = cnt.orderBy(F.desc("count")).first().pu
for z in (common, rare):
    row = spark.createDataFrame([(z,)], "pu double")
    a, b2 = te.transform(row).first().pu_te, te_s.transform(row).first().pu_te
    n = cnt.where(F.col("pu") == z).first()["count"]
    print(f"  zone {int(z):>3} ({n:>6} card trips): smoothing=0 -> {a:6.2f}   smoothing=100 -> {b2:6.2f}")
print(f"  mean tip percent of all card trips: {card.agg(F.avg('tip_pct')).first()[0]:.2f}")

print("\n== PCA on the 7 zone-profile columns (Step 1), after StandardScaler")
prof = zone_profiles(spark).orderBy("zone")
pp = Pipeline(stages=[VectorAssembler(inputCols=PROFILE_COLS, outputCol="raw"),
                      StandardScaler(inputCol="raw", outputCol="x", withMean=True),
                      PCA(k=3, inputCol="x", outputCol="pc")]).fit(prof)
pca = pp.stages[-1]
ev = pca.explainedVariance.toArray()
print(f"  explained variance of 3 components: {[round(float(x), 3) for x in ev]}  (sum {sum(ev):.3f})")
pc1 = pca.pc.toArray()[:, 0]
print("  loadings of component 1: " + ", ".join(f"{c} {w:+.2f}" for c, w in zip(PROFILE_COLS, pc1)))

print("\n== RFormula and SQLTransformer: a model input from a formula and from SQL")
rf = RFormula(formula="fare_amount ~ trip_distance + minutes + rate", handleInvalid="keep")
sqlt = SQLTransformer(statement="SELECT *, CAST(RatecodeID AS STRING) AS rate FROM __THIS__")
m = Pipeline(stages=[sqlt, rf]).fit(t.where("RatecodeID IS NOT NULL"))
r = m.transform(t.where("RatecodeID IS NOT NULL")).select("features", "label").first()
attrs = m.transform(t.limit(1)).schema["features"].metadata["ml_attr"]["attrs"]
names = [a["name"] for a in sorted((a for kind in attrs.values() for a in kind), key=lambda a: a["idx"])]
print(f"  features has {len(names)} slots: {names}")
print(f"  first row: label {r.label}, features {r.features}")

print("\n== UnivariateFeatureSelector: keep the 2 slots with the highest F-value against the fare")
feats = m.transform(t.where("RatecodeID IS NOT NULL"))
sel = UnivariateFeatureSelector(featuresCol="features", labelCol="label", outputCol="sel",
                                selectionMode="numTopFeatures").setFeatureType("continuous") \
    .setLabelType("continuous").setSelectionThreshold(2).fit(feats)
print(f"  selected slot indexes {sel.selectedFeatures} -> {[names[i] for i in sel.selectedFeatures]}")
spark.stop()
