"""Chapter 91: what a pandas-on-Spark DataFrame is: a Spark DataFrame, an index column and a column map.

Then the three default index types: their plans and their cost on 3.8 million rows.
Timings: median of 5 runs after a warmup. Run from code/ under the bench lock:
  flock $BOOK_DATA/.bench.lock uv run python ch91/internals.py
"""
import statistics
import sys
import time
import warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, plan, TRIPS

spark = session("ch91-internals", spark_ui_showConsoleProgress="false")
import pyspark.pandas as ps                                      # noqa: E402

warnings.simplefilter("ignore")
RUNS = 5
COLS = ["PULocationID", "payment_type", "fare_amount", "tip_amount"]

print("== 1. one pandas-on-Spark DataFrame, three layers")
psdf = ps.read_parquet(str(TRIPS), columns=COLS)
internal = psdf._internal
print("type:", type(psdf).__module__ + "." + type(psdf).__name__, " shape:", psdf.shape)
print("spark_frame columns:", internal.spark_frame.columns)
print("index columns:      ", internal.index_spark_column_names)
print("data columns:       ", internal.data_spark_column_names)
print("spark.frame():      ", psdf.spark.frame().columns, "(drops the index)")
print("spark.frame(index_col='idx'):", psdf.spark.frame(index_col="idx").columns[:2], "...")
print(psdf.head(3))

print("\n== 2. the plan of each default index type")
short = lambda txt: [l.split(" Batched")[0] for l in txt.splitlines() if l.strip() and "==" not in l]
for t in ("sequence", "distributed-sequence", "distributed"):
    ps.set_option("compute.default_index_type", t)
    p = ps.read_parquet(str(TRIPS), columns=COLS)
    print(f"-- {t}")
    print("\n".join(short(plan(p.spark.frame(index_col="idx"), "simple"))))
ps.reset_option("compute.default_index_type")


def timed(fn):
    fn()
    ts = []
    for _ in range(RUNS):
        s = time.perf_counter(); fn(); ts.append(time.perf_counter() - s)
    return statistics.median(ts)


print("\n== 3. the cost of each default index (median of 5 runs after a warmup)")
print(f"  {'index':34} {'all rows (noop write)':>22} {'head(5)':>9} {'fare mean':>10}")
for t in ("sequence", "distributed-sequence", "distributed", "index_col='PULocationID'"):
    def make():
        if t.startswith("index_col"):
            return ps.read_parquet(str(TRIPS), columns=COLS, index_col="PULocationID")
        ps.set_option("compute.default_index_type", t)
        return ps.read_parquet(str(TRIPS), columns=COLS)
    write = timed(lambda: make().spark.frame(index_col="idx").write.format("noop").mode("overwrite").save())
    head = timed(lambda: make().head(5).to_pandas())
    mean = timed(lambda: make()["fare_amount"].mean())
    print(f"  {t:34} {write:21.2f}s {head:8.2f}s {mean:9.2f}s")
ps.reset_option("compute.default_index_type")
spark_df = spark.read.parquet(str(TRIPS)).select(*COLS)
print(f"  {'(PySpark DataFrame, no index)':34} "
      f"{timed(lambda: spark_df.write.format('noop').mode('overwrite').save()):21.2f}s "
      f"{timed(lambda: spark_df.limit(5).toPandas()):8.2f}s "
      f"{timed(lambda: spark_df.agg({'fare_amount': 'avg'}).collect()):9.2f}s")
