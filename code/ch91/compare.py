"""Chapter 91: the same five operations in pandas, in pandas API on Spark and in PySpark, measured.

Data: the June 2026 taxi month, four columns, 3,837,248 rows. pandas reads it with PyArrow into
the driver; the two Spark versions read the Parquet file in each run.
Timings: median of 5 runs after a warmup. Run from code/ under the bench lock:
  flock $BOOK_DATA/.bench.lock uv run python ch91/compare.py
"""
import glob
import statistics
import sys
import time
import warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import functions as F
from common.book import session, TRIPS

spark = session("ch91-compare", spark_ui_showConsoleProgress="false")
import pyspark.pandas as ps                                      # noqa: E402

warnings.simplefilter("ignore")
RUNS = 5
COLS = ["PULocationID", "payment_type", "fare_amount", "tip_amount"]
files = sorted(glob.glob(str(TRIPS / "*.parquet")))

t = time.perf_counter()
pdf = pd.read_parquet(files, columns=COLS)
print(f"pandas read_parquet: {time.perf_counter() - t:.2f} s, {len(pdf):,} rows, "
      f"{pdf.memory_usage(deep=True).sum() / 2**20:.1f} MiB in the driver")
psdf = ps.read_parquet(str(TRIPS), columns=COLS)
sdf = spark.read.parquet(str(TRIPS)).select(*COLS)

ops = {
    "groupby mean": (
        lambda: pdf.groupby("PULocationID")["fare_amount"].mean().sort_index(),
        lambda: psdf.groupby("PULocationID")["fare_amount"].mean().sort_index().to_pandas(),
        lambda: sdf.groupBy("PULocationID").agg(F.avg("fare_amount")).orderBy("PULocationID").toPandas()),
    "value_counts": (
        lambda: pdf["payment_type"].value_counts(),
        lambda: psdf["payment_type"].value_counts().to_pandas(),
        lambda: sdf.groupBy("payment_type").count().orderBy(F.desc("count")).toPandas()),
    "describe": (
        lambda: pdf[["fare_amount", "tip_amount"]].describe(),
        lambda: psdf[["fare_amount", "tip_amount"]].describe().to_pandas(),
        lambda: sdf.select("fare_amount", "tip_amount").summary().toPandas()),
    "nlargest(5)": (
        lambda: pdf.nlargest(5, "fare_amount"),
        lambda: psdf.nlargest(5, "fare_amount").to_pandas(),
        lambda: sdf.orderBy(F.desc("fare_amount")).limit(5).toPandas()),
    "filter + new column": (
        lambda: pdf[pdf.fare_amount > 0].assign(pct=lambda d: d.tip_amount / d.fare_amount)["pct"].mean(),
        lambda: psdf[psdf.fare_amount > 0].assign(pct=lambda d: d.tip_amount / d.fare_amount)["pct"].mean(),
        lambda: sdf.where("fare_amount > 0").select(F.avg(F.col("tip_amount") / F.col("fare_amount"))).first()[0]),
}


def timed(fn):
    fn()
    ts = []
    for _ in range(RUNS):
        s = time.perf_counter(); fn(); ts.append(time.perf_counter() - s)
    return statistics.median(ts)


print("\nanswers agree (filter + new column):",
      round(ops["filter + new column"][0](), 6) == round(ops["filter + new column"][1](), 6)
      == round(ops["filter + new column"][2](), 6))
print(f"\n  {'operation':22} {'pandas':>8} {'pandas on Spark':>16} {'PySpark':>9}")
for name, (a, b, c) in ops.items():
    print(f"  {name:22} {timed(a):7.3f}s {timed(b):15.3f}s {timed(c):8.3f}s")
print(f"\nmedian of {RUNS} runs after a warmup, local[*], {spark.sparkContext.defaultParallelism} cores;"
      " pandas times exclude its read")
