"""Chapter 6: what a collation costs. Group 2,000,000 cached rows under four collations.

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
# The cost: group 2 million cached rows by city under each collation.
print("cost of grouping 2,000,000 cached rows (median of 5 after a warmup)")
spellings = ["Montréal", "MONTREAL", "montreal", "Toronto", "toronto", "Hà Nội", "Ha Noi", "HA NOI"]
big = (spark.range(2_000_000).select(F.element_at(F.array(*[F.lit(s) for s in spellings]),
                                                  (F.col("id") % len(spellings) + 1).cast("int")).alias("city"))
       .cache())
big.count()
for coll in ["UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI_AI"]:
    q = big.groupBy(F.collate("city", coll).alias("c")).count()
    times = []
    for _ in range(6):
        t0 = time.perf_counter()
        n = len(q.collect())
        times.append(time.perf_counter() - t0)
    print(f"  {coll:<14} {n} groups  {sorted(times[1:])[2]:.2f} s")
big.unpersist()

