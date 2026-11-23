"""Chapter 93, Step 8: model selection for ALS. CrossValidator, TrainValidationSplit, parallelism, foldCol.

The timings are quoted, so run under the bench lock:
  flock $HOME/spark-masterclass/data/.bench.lock uv run python ch93/tuning.py
"""
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.ml.evaluation import RegressionEvaluator  # noqa: E402
from pyspark.ml.recommendation import ALS  # noqa: E402
from pyspark.ml.tuning import CrossValidator, TrainValidationSplit, ParamGridBuilder  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from common.book import session  # noqa: E402
from ch93.taxi93 import trips  # noqa: E402

spark = session("ch93-tuning", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="16")
pairs = (trips(spark).where(F.col("tip_pct").isNotNull() & (F.col("tip_pct") <= 100))
         .groupBy(F.col("PULocationID").alias("pu"), F.col("DOLocationID").alias("do"))
         .agg(F.count("*").alias("n"), F.avg("tip_pct").alias("tip")).where("n >= 20")
         .orderBy("pu", "do").cache())
train, test = pairs.randomSplit([0.8, 0.2], seed=42)
train.cache().count()
als = ALS(userCol="pu", itemCol="do", ratingCol="tip", maxIter=10, seed=7, coldStartStrategy="drop")
grid = ParamGridBuilder().addGrid(als.rank, [4, 8, 16]).addGrid(als.regParam, [0.01, 0.1, 1.0]).build()
ev = RegressionEvaluator(labelCol="tip", metricName="rmse")
print(f"== grid: {len(grid)} settings (rank x regParam), {train.count():,} training pairs")

cv = CrossValidator(estimator=als, estimatorParamMaps=grid, evaluator=ev, numFolds=3, seed=7)
cvm = cv.fit(train)
print("CrossValidator, 3 folds: avgMetrics (rmse) and stdMetrics")
for pm, a, s in zip(grid, cvm.avgMetrics, cvm.stdMetrics):
    print(f"  rank {pm[als.rank]:>2}  regParam {pm[als.regParam]:<5}  rmse {a:.3f} +- {s:.3f}")
best = cvm.bestModel
print(f"best: rank {best.rank}, regParam {best._java_obj.parent().getRegParam()}"
      f"  test rmse {ev.evaluate(best.transform(test)):.3f}")

tvs = TrainValidationSplit(estimator=als, estimatorParamMaps=grid, evaluator=ev, trainRatio=0.75, seed=7)
tvm = tvs.fit(train)
i = min(range(len(grid)), key=lambda j: tvm.validationMetrics[j])
print(f"TrainValidationSplit: best rank {grid[i][als.rank]}, regParam {grid[i][als.regParam]}, "
      f"validation rmse {tvm.validationMetrics[i]:.3f}, test rmse {ev.evaluate(tvm.bestModel.transform(test)):.3f}")

# foldCol: put all pairs of one pickup zone in the same fold
folded = train.withColumn("fold", (F.col("pu") % 3).cast("int"))
try:
    CrossValidator(estimator=als, estimatorParamMaps=grid, evaluator=ev, foldCol="fold").fit(folded)
except Exception as e:
    print(f"foldCol by pickup zone: {type(e).__name__}: {str(e).splitlines()[0][:70]}")
    print("  every validation zone is new to the model, coldStartStrategy='drop' removes every row")
print(f"collectSubModels=False (default): subModels is {cvm.subModels}")

print("\n== parallelism: the same CrossValidator, 3 runs each after a warmup (local[*], 16 cores)")
for par in (1, 4):
    c = cv.copy().setParallelism(par)
    c.fit(train)                                  # warmup
    ts = []
    for _ in range(3):
        t0 = time.time(); c.fit(train); ts.append(time.time() - t0)
    print(f"  parallelism={par}  median {statistics.median(ts):5.1f} s  {[round(x, 1) for x in ts]}")
spark.stop()
