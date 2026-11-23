"""Chapter 72, Step 7 and 8: ids and samples that move with the partitioning, and writes that
survive a retry but not a rerun.

Run from code/:  uv run python ch72/writes.py
"""
import os
import sys
import tempfile
from pathlib import Path

from pyspark import TaskContext
from pyspark.sql import functions as F

from chaos import fresh
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import TRIPS  # noqa: E402

# --- Position-based ids and samples depend on how the input splits -------------------------------
for split in ("128m", "8m"):
    spark = fresh("local[4]", **{"spark.sql.files.maxPartitionBytes": split})
    trips = spark.read.parquet(str(TRIPS))
    keyed = trips.select("VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID",
                         "total_amount")
    row = (keyed.withColumn("mid", F.monotonically_increasing_id())
           .withColumn("content_id", F.xxhash64(*keyed.columns))
           .orderBy("tpep_pickup_datetime", "PULocationID", "DOLocationID", "total_amount")
           .where("tpep_pickup_datetime >= '2026-06-15 12:00:00'").first())
    print(f"maxPartitionBytes={split:4} scan tasks={trips.rdd.getNumPartitions():2}  "
          f"sample rows={trips.sample(fraction=0.001, seed=7).count():5}  "
          f"one trip: mid={row.mid:>12}  xxhash64={row.content_id}")

# The seed must go in as a keyword together with fraction=. Read the seed back from each plan.
for label, df in [("sample(fraction=0.001, seed=7)", trips.sample(fraction=0.001, seed=7)),
                  ("sample(0.001, 7)", trips.sample(0.001, 7)),
                  ("sample(0.001, seed=7)", trips.sample(0.001, seed=7))]:
    seed = df._jdf.queryExecution().logical().toString().split(",")[3].split()[0]
    print(f"{label:31} seed in the plan: {seed}")
spark.stop()

# --- The commit protocol, seen from inside a task -----------------------------------------------
spark = fresh("local[2,2]")
out = tempfile.mkdtemp(prefix="ch72-commit-")
seen = tempfile.mkdtemp(prefix="ch72-seen-")


def peek(x):
    ctx = TaskContext.get()
    if x in (0, 50):                                  # one row per task: list the output folder
        listing = []
        for root, _, files in os.walk(out):
            for f in files:
                if f.endswith(".parquet") or "attempt_" in root:
                    listing.append(os.path.relpath(os.path.join(root, f), out))
        with open(f"{seen}/p{ctx.partitionId()}_a{ctx.attemptNumber()}", "w") as fh:
            fh.write("\n".join(listing))
        if ctx.partitionId() == 1 and ctx.attemptNumber() == 0:
            raise RuntimeError("injected after the task started its write")
    return x


df = spark.range(0, 100, numPartitions=2).select(F.udf(peek, "long")("id").alias("id"))
df.write.mode("overwrite").parquet(out)
import re
short = lambda p: re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "<uuid>", re.sub(r"_\d{20,}_", "_<job>_", p))
for name in sorted(os.listdir(seen)):
    print(f"\n{name}: what the task saw under the output folder while it ran")
    for line in open(f"{seen}/{name}").read().splitlines() or ["(no files)"]:
        print("  ", short(line))
print("\nafter the job commit:", [short(f) for f in sorted(os.listdir(out)) if not f.startswith(".")])

# --- A rerun: the commit protocol makes one job atomic, not two jobs idempotent -----------------
rows = spark.range(0, 10).withColumn("day", F.when(F.col("id") < 5, "2026-06-01").otherwise("2026-06-02"))
for mode in ("append", "overwrite"):
    path = tempfile.mkdtemp(prefix=f"ch72-{mode}-")
    for _ in range(2):                               # the job, then the scheduler's rerun
        rows.write.mode(mode).parquet(path)
    print(f"{mode:9} run twice: {spark.read.parquet(path).count()} rows (10 expected)")

# Partitioned output: a rerun for one day under static and dynamic overwrite.
for how in ("static", "dynamic"):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", how)
    path = tempfile.mkdtemp(prefix=f"ch72-{how}-")
    rows.write.mode("overwrite").partitionBy("day").parquet(path)
    rows.where("day = '2026-06-02'").write.mode("overwrite").partitionBy("day").parquet(path)
    days = {r.day: r["count"] for r in spark.read.parquet(path).groupBy("day").count().collect()}
    print(f"partitionOverwriteMode={how:7} after re-running 2026-06-02: { {str(k): v for k, v in sorted(days.items())} }")
spark.stop()
