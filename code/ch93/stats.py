"""Chapter 93, Step 6: pyspark.ml.stat. Correlation, ChiSquareTest, Summarizer, on the trips.

Run from code/:  uv run python ch93/stats.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.ml.feature import VectorAssembler, StringIndexer  # noqa: E402
from pyspark.ml.stat import Correlation, ChiSquareTest, Summarizer  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from common.book import session  # noqa: E402
from ch93.taxi93 import trips, zones  # noqa: E402

spark = session("ch93-stats", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="16")
t = trips(spark).where(F.col("minutes").between(1, 180)).cache()
cols = ["trip_distance", "minutes", "fare_amount", "tip_amount"]
vec = VectorAssembler(inputCols=cols, outputCol="v").transform(t).select("v")
print(f"== Correlation over {t.count():,} trips: {cols}")
for method in ("pearson", "spearman"):
    m = Correlation.corr(vec, "v", method).head()[0].toArray()
    print(f"  {method}")
    for i, c in enumerate(cols):
        print(f"    {c:<14}" + " ".join(f"{m[i][j]:6.3f}" for j in range(len(cols))))
print(f"  F.corr(trip_distance, minutes) = {t.select(F.corr('trip_distance', 'minutes')).first()[0]:.3f}"
      "  (one pair, the same pearson number)")

print("\n== ChiSquareTest: is the payment type independent of the pickup borough?")
z = zones(spark).select(F.col("zone").alias("PULocationID"), "borough")
c = (t.where(F.col("payment_type").isin(1, 2)).join(z, "PULocationID")
      .where(F.col("borough").isin("Manhattan", "Queens", "Brooklyn", "Bronx")))
c = StringIndexer(inputCol="borough", outputCol="b").fit(c).transform(c)
c = VectorAssembler(inputCols=["b"], outputCol="features").transform(c).withColumn("label", F.col("payment_type").cast("double"))
r = ChiSquareTest.test(c, "features", "label").head()
print(f"  pValues {[float(x) for x in r.pValues]}  degreesOfFreedom {list(r.degreesOfFreedom)}  statistics {[round(float(x), 1) for x in r.statistics]}")
(c.groupBy("borough").pivot("payment_type", [1, 2]).count()
  .withColumn("cash_share", F.round(F.col("2") / (F.col("1") + F.col("2")), 3))
  .withColumnRenamed("1", "card").withColumnRenamed("2", "cash").orderBy("cash_share").show())
small = c.sample(fraction=0.0005, seed=1)
r2 = ChiSquareTest.test(small, "features", "label").head()
print(f"  on a 0.05% sample ({small.count()} trips): pValue {r2.pValues[0]:.4f}  statistic {r2.statistics[0]:.1f}")

print("== Summarizer: many statistics of a vector column in one pass")
s = Summarizer.metrics("mean", "variance", "min", "max", "count", "numNonZeros")
row = vec.select(s.summary(F.col("v")).alias("s")).first().s
for k in ("mean", "variance", "min", "max", "numNonZeros"):
    print(f"  {k:<12}" + " ".join(f"{x:10.2f}" for x in row[k]))
print(f"  count       {row['count']}")
w = VectorAssembler(inputCols=cols, outputCol="v").transform(t).withColumn("w", F.col("passenger_count").cast("double"))
w = w.where("w > 0")
print(f"  rows with passenger_count > 0: {w.count():,}")
wm = w.select(Summarizer.mean(F.col("v"), F.col("w")).alias("m")).first().m
print("  mean weighted by passenger_count " + " ".join(f"{x:10.2f}" for x in wm))
plain = w.select(*[F.round(F.avg(c), 2).alias(c) for c in cols]).first()
print("  F.avg on the same rows           " + " ".join(f"{plain[c]:10.2f}" for c in cols))
spark.stop()
