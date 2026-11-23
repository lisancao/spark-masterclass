"""Chapter 46: where single-task execution stops paying, measured across input sizes.

One Parquet file per size, cut from the June trips. Two query shapes on each file, timed with
spark.sql.optimizer.singleTaskExecution.enabled off and on, alternating run by run, a new DataFrame
each run, median of RUNS after one warm-up of each setting.
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch46/crossover.py
"""
import os, shutil, statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F, Window
from common.book import session, TRIPS

spark = session("ch46-crossover", spark_driver_memory="4g", spark_ui_showConsoleProgress="false")
FLAG = "spark.sql.optimizer.singleTaskExecution.enabled"
RUNS = int(os.environ.get("RUNS", 5))
DIR = Path("/tmp/masterclass-ch46/sizes")
SIZES = [1_000, 10_000, 100_000, 300_000, 1_000_000, 2_000_000]

june = spark.read.parquet(str(TRIPS))
total = june.count()
files = {}
if not DIR.exists():
    for n in SIZES:      # the first n rows, in one file (coalesce(1) keeps the file order)
        june.limit(n).coalesce(1).write.parquet(str(DIR / f"{n}"))
for n in SIZES:
    files[n] = str(DIR / f"{n}")
files[total] = str(TRIPS)

SHAPES = {
    "aggregate": lambda df: (df.groupBy("PULocationID").agg(F.count("*"), F.avg("fare_amount"))
                             .orderBy("PULocationID")),
    "window": lambda df: (df.withColumn("r", F.rank().over(
                              Window.partitionBy("PULocationID").orderBy("fare_amount")))
                          .agg(F.max("r"))),
}


def once(path, shape, enabled):
    spark.conf.set(FLAG, enabled)
    t = time.perf_counter()
    SHAPES[shape](spark.read.parquet(path)).collect()      # a new DataFrame each run
    return (time.perf_counter() - t) * 1000


print(f"cores: {os.cpu_count()}; runs per cell: {RUNS} after a warm-up; times are medians in ms\n")
print(f"{'rows':>10}{'MB':>7}  {'shape':<10}{'off':>9}{'on':>9}{'on/off':>8}")
for n, path in sorted(files.items()):
    mb = sum(f.stat().st_size for f in Path(path).glob("*.parquet")) / 2**20
    for shape in SHAPES:
        once(path, shape, "false"); once(path, shape, "true")              # warm-up
        off, on = [], []
        for _ in range(RUNS):
            off.append(once(path, shape, "false"))
            on.append(once(path, shape, "true"))
        a, b = statistics.median(off), statistics.median(on)
        print(f"{n:>10,}{mb:>7.1f}  {shape:<10}{a:>9.0f}{b:>9.0f}{b / a:>8.2f}", flush=True)
