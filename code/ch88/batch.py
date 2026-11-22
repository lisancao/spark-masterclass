"""Write speed by batchsize, and with the driver's reWriteBatchedInserts, into PostgreSQL.

Run under the bench lock. 200,000 taxi rows in 4 tasks into a new table, one warm-up write,
then the median of three writes for each setting. The server logs no statements during the runs.
"""
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jdbc import PROPS, URL, pg_session, psql  # noqa: E402

ROWS, TASKS, RUNS = 200_000, 4, 3
spark = pg_session("ch88-batch")
df = spark.table("pg.public.trips").drop("trip_id").limit(ROWS).repartition(TASKS).cache()
df.count()

settings = [
    ("batchsize=10", URL, {"batchsize": "10"}),
    ("batchsize=100", URL, {"batchsize": "100"}),
    ("batchsize=1000 (default)", URL, {}),
    ("batchsize=10000", URL, {"batchsize": "10000"}),
    ("batchsize=1000, reWriteBatchedInserts=true", URL + "?reWriteBatchedInserts=true", {}),
    ("batchsize=10000, reWriteBatchedInserts=true", URL + "?reWriteBatchedInserts=true", {"batchsize": "10000"}),
]

psql("ALTER SYSTEM SET log_statement = 'none'; SELECT pg_reload_conf();")
try:
    df.write.jdbc(URL, "batch_test", mode="overwrite", properties=PROPS)        # warm-up
    print(f"{ROWS:,} rows, {TASKS} tasks, local[4], PostgreSQL 16 in Docker; median of {RUNS} writes\n")
    print(f"{'setting':46} {'median s':>9} {'rows/s':>9}   runs")
    for name, url, opts in settings:
        times = []
        for _ in range(RUNS):
            t = time.perf_counter()
            df.write.jdbc(url, "batch_test", mode="overwrite", properties={**PROPS, **opts})
            times.append(time.perf_counter() - t)
        med = statistics.median(times)
        print(f"{name:46} {med:9.2f} {ROWS / med:9,.0f}   " + " ".join(f"{x:.2f}" for x in times))
finally:
    psql("ALTER SYSTEM SET log_statement = 'all'; SELECT pg_reload_conf();")
print("\nrows in batch_test:", psql("SELECT count(*) FROM batch_test").split()[2])
