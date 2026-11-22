"""Chapter 6: what a collation costs. Group 20,000,000 cached rows under four collations.

Timings: run under the bench lock.
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch06/collation_cost.py
"""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session

spark = session("ch06-collation-cost")
# The cost: group 20 million cached rows by city under each collation.
print("cost of grouping 20,000,000 cached rows (median of 5 after a warmup)")
spellings = ["Montréal", "MONTREAL", "montreal", "Toronto", "toronto", "Hà Nội", "Ha Noi", "HA NOI"]
big = (spark.range(20_000_000).select(F.element_at(F.array(*[F.lit(s) for s in spellings]),
                                                  (F.col("id") % len(spellings) + 1).cast("int")).alias("city"))
       .cache())
big.count()
# Build a new DataFrame for each run. A second collect() on the same DataFrame
# reuses its shuffle files and skips the map stage, so it measures almost nothing.
def run(coll):
    t0 = time.perf_counter()
    n = len(big.groupBy(F.collate("city", coll).alias("c")).count().collect())
    return n, time.perf_counter() - t0

COLLS = ["UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI_AI"]
for coll in COLLS:          # warmup: JIT and the ICU collators
    run(coll)
for coll in COLLS:
    res = [run(coll) for _ in range(5)]
    print(f"  {coll:<14} {res[0][0]} groups  {sorted(t for _, t in res)[2]:.2f} s")
big.unpersist()

