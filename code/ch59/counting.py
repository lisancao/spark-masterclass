"""Chapter 59, Step 6: accumulators against retries and second actions, and observe().

Run from code/:  uv run python ch59/counting.py
"""
import glob
import tempfile

from pyspark import TaskContext
from pyspark.sql import Observation, functions as F

from chaos import fresh

spark = fresh("local[4,3]")
calls = spark.sparkContext.accumulator(0)


def counted(x):
    calls.add(1)
    if TaskContext.get().attemptNumber() == 0 and x % 100 == 50:
        raise RuntimeError("injected")                 # every task fails once, half way through
    return x


u = F.udf(counted, "long")
df = spark.range(0, 400, numPartitions=4).select(u("id").alias("v"))
out = tempfile.mkdtemp(prefix="ch59-out-")

df.agg(F.sum("v")).first()
print("one action, every task retried once:", calls.value)
df.write.mode("overwrite").parquet(out)
print("after a second action:              ", calls.value)

calls.value = 0
obs = Observation("m")
observed = df.observe(obs, F.count(F.lit(1)).alias("rows"), F.sum("v").alias("total"))
observed.write.mode("overwrite").parquet(out)
print("observe() on the write:", obs.get, " accumulator:", calls.value)
print("part files:", len(glob.glob(f"{out}/part-*")), " rows read back:", spark.read.parquet(out).count())
print("CollectMetrics in the plan:",
      "CollectMetrics" in observed._jdf.queryExecution().executedPlan().toString())

# cache() makes the second action cheap, not deterministic.
calls.value = 0
cached = spark.range(0, 400, numPartitions=4).select(u("id").alias("v")).cache()
cached.count()
cached.agg(F.sum("v")).first()
print("cached: two actions, UDF calls:", calls.value)
spark.stop()
