"""Chapter 64, Step 4: ALS on origin-destination pairs. Pickup zones are the users, dropoff zones the items.

Explicit: predict the mean tip percent of a pair that the model did not see.
Implicit: trip counts are confidence, rank the destinations of each pickup zone.
Run from code/:  uv run python ch64/als.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.ml.evaluation import RegressionEvaluator, RankingEvaluator  # noqa: E402
from pyspark.ml.recommendation import ALS  # noqa: E402
from pyspark.sql import functions as F, Window  # noqa: E402
from common.book import session  # noqa: E402
from ch64.taxi93 import trips, zones  # noqa: E402

spark = session("ch64-als", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="16")
t = trips(spark).select("PULocationID", "DOLocationID", "tip_pct", "tpep_pickup_datetime").cache()

# ---- explicit: one "rating" per pair = mean tip percent of its card trips (pairs with >= 20 card trips)
pairs = (t.where(F.col("tip_pct").isNotNull() & (F.col("tip_pct") <= 100))
          .groupBy(F.col("PULocationID").alias("pu"), F.col("DOLocationID").alias("do"))
          .agg(F.count("*").alias("n"), F.avg("tip_pct").alias("tip"))
          .where("n >= 20").cache())
train, test = pairs.randomSplit([0.8, 0.2], seed=42)
print(f"== explicit: {pairs.count():,} pairs with >= 20 card trips, "
      f"{pairs.select('pu').distinct().count()} pickup zones, {pairs.select('do').distinct().count()} dropoff zones")
ev = RegressionEvaluator(labelCol="tip", predictionCol="prediction", metricName="rmse")
als = ALS(userCol="pu", itemCol="do", ratingCol="tip", rank=8, regParam=0.1, maxIter=15, seed=7)
m = als.fit(train)
nan_rmse = ev.evaluate(m.transform(test))
print(f"  coldStartStrategy='nan' (default): rmse {nan_rmse:.3f}   "
      f"rows with NaN prediction: {m.transform(test).where(F.isnan('prediction')).count()}")
m = als.setColdStartStrategy("drop").fit(train)
pred = m.transform(test)
print(f"  coldStartStrategy='drop':          rmse {ev.evaluate(pred):.3f}   rows scored: {pred.count():,} of {test.count():,}")
gmean = train.agg(F.avg("tip")).first()[0]
pu_mean = train.groupBy("pu").agg(F.avg("tip").alias("pu_mean"))
base = pred.join(pu_mean, "pu").withColumn("g", F.lit(gmean))
for name, col in (("global mean of train", "g"), ("mean of the pickup zone", "pu_mean"), ("ALS rank 8", "prediction")):
    r = ev.setPredictionCol(col).evaluate(base)
    print(f"  {name:<26} rmse {r:.3f}")
ev.setPredictionCol("prediction")
print(f"  factors: userFactors {m.userFactors.count()} rows, itemFactors {m.itemFactors.count()} rows, "
      f"rank {m.rank}")

# ---- implicit: counts per pair; train on June 1-21, test on June 22-30
print("\n== implicit: trip counts as confidence (train June 1-21, test June 22-30)")
cut = F.lit("2026-06-22").cast("timestamp")
def counts(df):
    return (df.groupBy(F.col("PULocationID").alias("pu"), F.col("DOLocationID").alias("do"))
              .agg(F.count("*").cast("float").alias("trips")))
tr = counts(t.where(F.col("tpep_pickup_datetime") < cut)).cache()
te = counts(t.where(F.col("tpep_pickup_datetime") >= cut)).cache()
print(f"  train pairs {tr.count():,}  test pairs {te.count():,}")
K = 10
w = Window.partitionBy("pu").orderBy(F.desc("trips"), "do")
truth = (te.withColumn("r", F.row_number().over(w)).where(f"r <= {K}")
           .groupBy("pu").agg(F.sort_array(F.collect_list(F.struct("r", F.col("do").cast("double").alias("do")))).alias("s"))
           .select("pu", F.col("s.do").alias("label")))
users = truth.select("pu")
def as_lists(df, score):
    ww = Window.partitionBy("pu").orderBy(F.desc(score), "do")
    return (df.withColumn("r", F.row_number().over(ww)).where(f"r <= {K}")
              .groupBy("pu").agg(F.sort_array(F.collect_list(F.struct("r", F.col("do").cast("double").alias("do")))).alias("s"))
              .select("pu", F.col("s.do").alias("prediction")))
rank_ev = RankingEvaluator(k=K)
def score(name, lists, who=truth):
    j = who.join(lists, "pu")
    p = rank_ev.setMetricName("precisionAtK").evaluate(j)
    n = rank_ev.setMetricName("ndcgAtK").evaluate(j)
    print(f"  {name:<38} precision@10 {p:.3f}  ndcg@10 {n:.3f}  ({j.count()} pickup zones)")
    return n

pop = tr.groupBy("do").agg(F.sum("trips").alias("trips"))
popular = as_lists(users.crossJoin(pop), "trips")
own = as_lists(tr.join(users, "pu"), "trips")
score("most popular destinations", popular)
score("own top destinations in train", own)
tr_log = tr.withColumn("trips", F.log1p("trips"))
best, best_n = None, -1
for label, data in (("counts", tr), ("log1p(counts)", tr_log)):
    for alpha in (1.0, 40.0):
        m = ALS(userCol="pu", itemCol="do", ratingCol="trips", implicitPrefs=True, alpha=alpha,
                rank=16, regParam=0.1, maxIter=15, seed=7).fit(data)
        lists = (m.recommendForUserSubset(users, K)
                 .select("pu", F.col("recommendations.do").cast("array<double>").alias("prediction")))
        n = score(f"ALS implicit, {label}, alpha={alpha:g}", lists)
        if n > best_n:
            best, best_n, best_lists, best_name = m, n, lists, f"{label}, alpha={alpha:g}"
imp = best

sparse = truth.join(tr.groupBy("pu").agg(F.sum("trips").alias("n")).where("n < 100"), "pu").drop("n")
print(f"\n  pickup zones with fewer than 100 train trips ({sparse.count()}):")
score("own top destinations in train", own, sparse)
score(f"ALS implicit, {best_name}", best_lists, sparse)

# one pickup zone, three lists
z = zones(spark)
names = {r.zone: r.zone_name for r in z.collect()}
ZONE = 138   # LaGuardia Airport
own = [r.do for r in tr.where(F.col("pu") == ZONE).orderBy(F.desc("trips"), "do").limit(5).collect()]
als5 = [r.do for r in imp.recommendForUserSubset(spark.createDataFrame([(ZONE,)], "pu int"), 5)
        .select(F.explode("recommendations.do").alias("do")).collect()]
real = [r.do for r in te.where(F.col("pu") == ZONE).orderBy(F.desc("trips"), "do").limit(5).collect()]
print(f"\n  pickup zone {ZONE} ({names[ZONE]}), top 5:")
for label, lst in (("own history", own), (f"ALS", als5), ("test week", real)):
    print(f"    {label:<12} " + ", ".join(names.get(d, str(d)) for d in lst))
spark.stop()
