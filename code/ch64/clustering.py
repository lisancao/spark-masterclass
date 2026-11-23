"""Chapter 64, Steps 1-3: cluster the pickup zones by how their trips look.

KMeans over k, BisectingKMeans, GaussianMixture, and the 2-D KMeans history that the animation draws.
Run from code/:  uv run python ch64/clustering.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.clustering import KMeans, BisectingKMeans, GaussianMixture  # noqa: E402
from pyspark.ml.evaluation import ClusteringEvaluator  # noqa: E402
from pyspark.ml.feature import VectorAssembler, StandardScaler  # noqa: E402
from pyspark.ml.functions import vector_to_array  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from common.book import session  # noqa: E402
from ch64.taxi93 import zone_profiles, PROFILE_COLS  # noqa: E402

spark = session("ch64-clustering", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="16")
prof = zone_profiles(spark).orderBy("zone").cache()   # a fixed row order: KMeans|| init depends on it
print(f"zones with at least 500 pickups: {prof.count()}")

prep = Pipeline(stages=[VectorAssembler(inputCols=PROFILE_COLS, outputCol="raw"),
                        StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True)])
feats = prep.fit(prof).transform(prof).cache()

print("\n== KMeans: for each k, the lowest-cost model of seeds 1-5")
ev = ClusteringEvaluator()          # silhouette, squared euclidean distance
best = {}
for k in range(2, 9):
    runs = [KMeans(k=k, seed=s).fit(feats) for s in range(1, 6)]
    costs = [m.summary.trainingCost for m in runs]
    m = best[k] = runs[costs.index(min(costs))]
    s = ev.evaluate(m.transform(feats))
    print(f"  k={k}  silhouette {s:.3f}  cost {min(costs):6.1f} (seeds: {min(costs):.0f} to {max(costs):.0f})"
          f"  sizes {sorted(m.summary.clusterSizes, reverse=True)}")

K = 3   # the best silhouette above
km = best[K]
out = km.transform(feats)
print(f"\n== KMeans k={K}: cluster sizes and mean profile (unscaled)")
summary = (out.groupBy("prediction")
           .agg(F.count("*").alias("zones"), F.sum("trips").alias("trips"),
                *[F.round(F.avg(c), 2).alias(c) for c in ["avg_miles", "avg_minutes", "night_share",
                                                          "card_share", "avg_tip_pct"]])
           .orderBy("prediction"))
summary.show(truncate=False)
w = F.row_number().over(__import__("pyspark.sql.window", fromlist=["Window"]).Window
                        .partitionBy("prediction").orderBy(F.desc("trips")))
top = (out.withColumn("r", w).where("r <= 3").groupBy("prediction")
       .agg(F.concat_ws(", ", F.collect_list("zone_name")).alias("busiest zones")).orderBy("prediction"))
small = out.groupBy("prediction").count().orderBy("count").first().prediction
top.show(truncate=False)
print(f"the smallest cluster ({small}):")
(out.where(F.col("prediction") == small)
    .select("zone_name", "borough", "trips", F.round("avg_miles", 1).alias("miles"),
            F.round("card_share", 2).alias("card_share"), F.round("avg_tip_pct", 1).alias("tip_pct"))
    .show(truncate=False))

print(f"== BisectingKMeans k={K} (top-down splits) against KMeans")
bk = BisectingKMeans(k=K, seed=7).fit(feats)
print(f"  centered features (withMean=True), {feats.rdd.getNumPartitions()} partition(s): sizes {bk.summary.clusterSizes}")
for n in (2, 4, 8, 32, 200):
    bkn = BisectingKMeans(k=K, seed=7).fit(feats.repartition(n))
    print(f"  centered features (withMean=True), repartition({n}): sizes {bkn.summary.clusterSizes}")
root = [sum(c) / feats.count() for c in zip(*[r.features for r in feats.select("features").collect()])]
print(f"  norm of the mean of the centered features: {sum(x * x for x in root) ** 0.5:.1e}")
unc = StandardScaler(inputCol="raw", outputCol="features_nc", withMean=False, withStd=True)
feats = unc.fit(feats).transform(feats)
bk = BisectingKMeans(k=K, seed=7, featuresCol="features_nc").fit(feats)
bout = bk.transform(feats)
evn = ClusteringEvaluator(featuresCol="features_nc")
km_nc = KMeans(k=K, seed=7, featuresCol="features_nc").fit(feats)
kout = km_nc.transform(feats)
print("  on features scaled without centering (withMean=False):")
print(f"    KMeans          silhouette {evn.evaluate(kout):.3f}  sizes {sorted(km_nc.summary.clusterSizes, reverse=True)}")
print(f"    BisectingKMeans silhouette {evn.evaluate(bout):.3f}  sizes {sorted(bk.summary.clusterSizes, reverse=True)}")
agree = (kout.select("zone", F.col("prediction").alias("a")).join(bout.select("zone", F.col("prediction").alias("b")), "zone")
         .groupBy("a", "b").count().orderBy("a", F.desc("count")))
print("    zones per (KMeans cluster, BisectingKMeans cluster):",
      [(r.a, r.b, r["count"]) for r in agree.collect()])

print(f"\n== GaussianMixture k={K}: soft assignment")
gm = GaussianMixture(k=K, seed=7, maxIter=200).fit(feats)
g = gm.transform(feats).withColumn("p_max", F.array_max(vector_to_array("probability")))
print(f"  weights {[round(x, 3) for x in gm.weights]}  log-likelihood {gm.summary.logLikelihood:.1f}"
      f"  iterations {gm.summary.numIter}")
b = g.select(F.count("*").alias("zones"),
             F.sum((F.col("p_max") >= 0.99).cast("int")).alias("p_max >= 0.99"),
             F.sum((F.col("p_max") < 0.9).cast("int")).alias("p_max < 0.9")).first()
print(f"  {b['zones']} zones: {b['p_max >= 0.99']} with p_max >= 0.99, {b['p_max < 0.9']} with p_max < 0.9")
(g.where("p_max < 0.9").orderBy("p_max")
  .select("zone_name", "borough", F.round("p_max", 3).alias("p_max"),
          F.transform(vector_to_array("probability"), lambda x: F.round(x, 2)).alias("probability"))
  .show(5, truncate=False))

# 2-D history for the animation: avg_miles and night_share, scaled, KMeans k=3, maxIter = 1..N
two = Pipeline(stages=[VectorAssembler(inputCols=["avg_miles", "night_share"], outputCol="raw"),
                       StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True)])
f2 = two.fit(prof).transform(prof).cache()
pts = [list(r.features) for r in f2.select("features").collect()]
hist = []
for it in range(1, 9):
    m = KMeans(k=3, seed=3, initMode="random", maxIter=it, tol=0.0).fit(f2)
    hist.append({"iter": it, "centers": [list(map(float, c)) for c in m.clusterCenters()],
                 "cost": m.summary.trainingCost})
print("\n== 2-D KMeans history (avg_miles, night_share scaled; k=3, initMode=random)")
for h in hist:
    print(f"  maxIter={h['iter']}  cost {h['cost']:7.2f}  centers "
          + " ".join(f"({c[0]:+.2f},{c[1]:+.2f})" for c in h["centers"]))
(Path(__file__).parent / "output" / "kmeans_history.json").write_text(json.dumps({"points": pts, "history": hist}))
spark.stop()
