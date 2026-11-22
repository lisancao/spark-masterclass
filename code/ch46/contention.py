"""Chapter 46, Step 3: two sessions share the executors of one server. A long query in session A,
a small query in session B, with the scheduler in FIFO mode and in FAIR mode.

Run from code/:  flock $HOME/spark-masterclass/data/.bench.lock uv run python ch46/contention.py
"""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from connect_util import server, session, close, stop  # noqa: E402
from pyspark.sql.connect import functions as F  # noqa: E402
from pyspark.sql.types import LongType  # noqa: E402

HERE = Path(__file__).resolve().parent


def nap(x):
    import time
    time.sleep(1.0)
    return x


def trial(url):
    a, b = session(url, user_id="batch"), session(url, user_id="dashboard")
    b.range(0, 10, 1, 2).groupBy(F.col("id") % 2).count().collect()   # warm up B
    slow = F.udf(nap, LongType())
    long_q = a.range(0, 16, numPartitions=16).select(F.sum(slow("id")))
    t = threading.Thread(target=long_q.collect)
    start = time.perf_counter()
    t.start()
    time.sleep(2.0)                                          # A now holds both task slots
    s = time.perf_counter()
    b.range(0, 10, 1, 2).groupBy(F.col("id") % 2).count().collect()
    waited = time.perf_counter() - s
    t.join()
    total = time.perf_counter() - start
    close(a, b)
    return waited, total


print("server local[2]; session A: 16 tasks of 1 s; session B: a small group-by, 2 s after A starts")
for mode, confs in [("FIFO", ["spark.master=local[2]"]),
                    ("FAIR", ["spark.master=local[2]", "spark.scheduler.mode=FAIR",
                              f"spark.scheduler.allocation.file={HERE / 'fairscheduler.xml'}"])]:
    url = server(*confs)
    for run in range(3):
        waited, total = trial(url)
        print(f"  {mode} run {run + 1}: B's query {waited:5.2f} s   A's query {total:5.2f} s")
stop()
