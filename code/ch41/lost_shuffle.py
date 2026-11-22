"""Chapter 41, Step 4 and 5: determinism levels in the DAG, and a lost shuffle three ways.

Run from code/:  uv run python ch41/lost_shuffle.py
"""
import glob
import os
import random
import shutil
import tempfile

from pyspark.sql import functions as F

from chaos import fresh
from common.book import ui_api

# --- Determinism levels, as the scheduler sees them -------------------------------------------
spark = fresh("local[4]", **{"spark.sql.adaptive.enabled": "false"})


def level(df):
    return df._jdf.queryExecution().toRdd().outputDeterministicLevel().toString()


base = spark.range(0, 1000, numPartitions=4)
grouped = base.groupBy((F.col("id") % 10).alias("k")).count()
for name, df in {
    "range": base,
    "groupBy": grouped,
    "hash repartition after groupBy": grouped.repartition(4, "k"),
    "round-robin repartition after groupBy": grouped.repartition(3),
    "sample after groupBy": grouped.sample(0.5, seed=1),
}.items():
    print(f"{name:42} {level(df)}")
spark.conf.set("spark.sql.execution.sortBeforeRepartition", "false")
print(f"{'round-robin, sortBeforeRepartition=false':42} {level(grouped.repartition(3))}")


# --- A lost shuffle -----------------------------------------------------------------------------
def lost_shuffle(udf_kind, full_retry):
    local = tempfile.mkdtemp(prefix="ch41-local-")
    spark = fresh("local[4]", **{
        "spark.local.dir": local,
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch": full_retry})

    def bucket(x):
        return random.randint(0, 3) if udf_kind == "random" else x % 4

    df = (spark.range(0, 4000, numPartitions=4)
          .withColumn("b", F.udf(bucket, "int")("id"))
          .repartition(4, "b")
          .groupBy("b").agg(F.count("*").alias("n")))
    first = sorted((r.b, r.n) for r in df.collect())
    victim = sorted(glob.glob(f"{local}/**/shuffle_*_0_0.data", recursive=True))[0]
    for f in glob.glob(victim.replace(".data", ".*")):
        os.remove(f)                                       # map task 0's output is gone
    label = f"{udf_kind:7} fullRetry={full_retry:5}"
    try:
        second = sorted((r.b, r.n) for r in df.collect())
        print(f"{label} same result: {first == second!s:5}  rows: {sum(n for _, n in second)}")
        print(f"{'':24}first  {first}\n{'':24}second {second}")
    except Exception as e:
        msg = next(l for l in str(e).splitlines() if "Job aborted" in l)
        msg = msg[msg.index("Job aborted"):]
        print(f"{label} FAILED:")
        words, line = msg.split(), "   "
        for w in words:
            if len(line) + len(w) > 86:
                print(line)
                line = "   "
            line += " " + w
        print(line)
    for st in sorted(ui_api("stages"), key=lambda st: (st["stageId"], st["attemptId"])):
        if st["status"] == "SKIPPED" or st["stageId"] < 2:
            continue                                         # stages 0 and 1: the first collect()
        ran = len(ui_api(f"stages/{st['stageId']}/{st['attemptId']}/taskList"))
        print(f"{'':24}stage {st['stageId']} attempt {st['attemptId']}: {st['status']:8} "
              f"{ran} task(s) ran")
    spark.stop()
    shutil.rmtree(local, ignore_errors=True)


print()
lost_shuffle("modulo", "true")
lost_shuffle("random", "false")
lost_shuffle("random", "true")
