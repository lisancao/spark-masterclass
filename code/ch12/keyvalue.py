"""Chapter 12: reduceByKey against groupByKey on 3.8 million (zone, fare) pairs, measured.

Both give the total fare for each pickup zone. reduceByKey combines inside each map task
before the shuffle. groupByKey ships every value across the shuffle and sums after it.
Time: median of 5 runs after one warmup. Shuffle bytes and records: the Spark UI REST API.
Run from code/ under the bench lock:
  flock $BOOK_DATA/.bench.lock uv run python ch12/keyvalue.py
"""
import statistics
import sys
import time
from operator import add
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, ui_api, TRIPS

spark = session("ch12-keyvalue", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
PORT = sc.uiWebUrl.rsplit(":", 1)[1]                        # the port this app got
RUNS = 5

pairs = (spark.read.parquet(str(TRIPS)).select("PULocationID", "fare_amount")
         .where("fare_amount IS NOT NULL").rdd.map(tuple)).cache()
n = pairs.count()


def shuffle_of(fn):
    """Run fn once and sum the shuffle metrics of the stages it ran (from the REST API)."""
    seen = {s["stageId"] for s in ui_api("stages", PORT)}
    out = fn()
    time.sleep(1.5)                                           # let the listener bus catch up
    new = [s for s in ui_api("stages", PORT) if s["stageId"] not in seen and s["status"] == "COMPLETE"]
    return out, {k: sum(s[k] for s in new) for k in
                 ("shuffleWriteBytes", "shuffleWriteRecords", "shuffleReadRecords")}


def timed(fn):
    fn()
    ts = []
    for _ in range(RUNS):
        t = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t)
    return statistics.median(ts), min(ts), max(ts)


by_reduce = lambda: pairs.reduceByKey(add).collect()
by_group = lambda: pairs.groupByKey().mapValues(sum).collect()
by_aggregate = lambda: pairs.aggregateByKey(0.0, add, add).collect()

r_out, r_sh = shuffle_of(by_reduce)
g_out, g_sh = shuffle_of(by_group)
a_out, a_sh = shuffle_of(by_aggregate)
same = {k: round(v, 2) for k, v in r_out} == {k: round(v, 2) for k, v in g_out} == \
       {k: round(v, 2) for k, v in a_out}
print(f"{n:,} (zone, fare) pairs in {pairs.getNumPartitions()} partitions, cached")
print(f"same totals from all three: {same}   zones: {len(r_out)}\n")

print(f"{'':16} {'median':>8} {'min-max':>13} {'shuffle write':>14} {'records out':>12}")
for name, fn, sh in (("reduceByKey", by_reduce, r_sh), ("aggregateByKey", by_aggregate, a_sh),
                     ("groupByKey", by_group, g_sh)):
    med, lo, hi = timed(fn)
    print(f"{name:16} {med:7.2f}s {lo:5.2f}-{hi:5.2f} s {sh['shuffleWriteBytes'] / 2**20:10.2f} MiB"
          f" {sh['shuffleWriteRecords']:>12,}")
print(f"\nmedian of {RUNS} runs after a warmup, local[*], {sc.defaultParallelism} cores")
