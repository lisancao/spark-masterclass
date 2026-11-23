"""Chapter 72, Step 1 and 2: local mode never retries a task, local[N,F] does, and which values
a retried task reproduces.

Run from code/:  uv run python ch72/attempts.py
"""
import glob
import os
import random
import tempfile

from pyspark import TaskContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from chaos import fresh, first_line


def attempt_demo(master, **conf):
    spark = fresh(master, **conf)
    log = tempfile.mkdtemp(prefix="ch72-attempts-")

    def flaky(x, r):
        ctx = TaskContext.get()
        v = random.random()                          # Python's own generator
        with open(f"{log}/p{ctx.partitionId()}_a{ctx.attemptNumber()}.txt", "a") as f:
            f.write(f"{x} {v} {r}\n")
        if ctx.partitionId() == 1 and ctx.attemptNumber() == 0 and x == 150:
            raise RuntimeError("injected failure on the first attempt")
        return v

    df = (spark.range(0, 400, numPartitions=4)
          .select("id", F.rand(7).alias("r"))        # Spark's generator, seed 7
          .withColumn("py", F.udf(flaky, DoubleType())("id", "r")))
    label = master + "".join(f" {k}={v}" for k, v in conf.items())
    try:
        print(f"{label}: {len(df.collect())} rows")
    except Exception as e:
        print(f"{label}: failed, {type(e).__name__}")
    print("  attempt files:", " ".join(sorted(os.path.basename(p)[:-4] for p in glob.glob(f"{log}/*"))))
    return log


attempt_demo("local[4]")
attempt_demo("local[4]", **{"spark.task.maxFailures": "4"})
log = attempt_demo("local[4,2]")

print("\nRow 120 lives in partition 1, so it ran in both attempts:")
for a in (0, 1):
    _, py, r = next(l for l in open(f"{log}/p1_a{a}.txt") if l.startswith("120 ")).split()
    print(f"  attempt {a}: random.random() = {float(py):.6f}   rand(7) = {float(r):.6f}")

spark = fresh("local[4,2]")
print("\nspark.task.maxFailures as the scheduler sees it:",
      spark.sparkContext._jsc.sc().taskScheduler().maxTaskFailures())
spark.stop()
