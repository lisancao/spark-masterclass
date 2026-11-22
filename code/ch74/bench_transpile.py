"""Chapter 74, Step 13: what transpilation saves on 3.8 million taxi trips.

One warm-up run, then the median of five fresh DataFrames, for each way of computing the same fare band.
Run from code/, on a quiet machine:
  flock $BOOK_DATA/.bench.lock uv run python ch74/bench_transpile.py
"""
import os, statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS

spark = session("ch74-bench-transpile")
trips = spark.read.parquet(str(TRIPS)).select("fare_amount").where("fare_amount IS NOT NULL")

def band(fare):
    return "invalid" if fare <= 0 else "short" if fare < 10 else "long"

def build(kind):
    spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", str(kind == "transpiled").lower())
    if kind == "native":
        col = F.when(F.col("fare_amount") <= 0, "invalid").when(F.col("fare_amount") < 10, "short") \
               .otherwise("long")
    else:
        u = F.udf(band, "string", useArrow=(kind == "Arrow @udf (101)"))
        col = u("fare_amount")
    return trips.groupBy(col.alias("band")).count()

print(f"{trips.count():,} trips, local[*] on {os.cpu_count()} cores, pyspark 4.3.0\n")
for kind in ["pickled @udf (100)", "Arrow @udf (101)", "transpiled", "native"]:
    # A fresh DataFrame for every run: collecting the same DataFrame again reuses its shuffle files.
    build(kind).collect()                        # warm-up
    times = []
    for _ in range(5):
        q = build(kind)
        t = time.perf_counter(); rows = q.collect(); times.append(time.perf_counter() - t)
    py = "ArrowEvalPython" in plan(q) or "BatchEvalPython" in plan(q)
    res = {r["band"]: r["count"] for r in rows}
    print(f"  {kind:<20} median {statistics.median(times):6.2f} s  Python node: {str(py):<5}  "
          f"short {res['short']:,}  long {res['long']:,}  invalid {res['invalid']:,}")
