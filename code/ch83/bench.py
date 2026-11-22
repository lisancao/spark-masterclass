"""Chapter 83: timings. Run under the bench lock:
  flock $BOOK_DATA/.bench.lock uv run python ch83/bench.py
Each case: one warmup, then 5 timed runs; the median is reported. Writes to the noop sink.
"""
import os
import platform
import statistics
import time
from aggbook import start

spark = start("ch83-bench", spark_driver_memory="4g")
spark.sql("CACHE TABLE j AS SELECT * FROM june")   # read the file one time, then time only the aggregates
spark.table("j").count()


def timed(sql, conf=None, n=5):
    for k, v in (conf or {}).items():
        spark.conf.set(k, v)
    df = spark.sql(sql)
    df.write.format("noop").mode("overwrite").save()          # warmup
    ts = []
    for _ in range(n):
        t = time.perf_counter()
        spark.sql(sql).write.format("noop").mode("overwrite").save()
        ts.append(time.perf_counter() - t)
    for k in (conf or {}):
        spark.conf.unset(k)
    return statistics.median(ts), min(ts), max(ts)


print(f"== conditions: local[*] on {os.cpu_count()} cores, {platform.python_version()}, driver 4g, "
      f"3,663,191 cached rows, shuffle partitions 200, median of 5 after 1 warmup")
CASES = [
    ("high-card key, partial + final", "SELECT tpep_pickup_datetime, count(*) FROM j GROUP BY 1", {}),
    ("high-card key, bypass partial", "SELECT tpep_pickup_datetime, count(*) FROM j GROUP BY 1",
     {"spark.sql.execution.bypassPartialAggregation": "true"}),
    ("low-card key, partial + final", "SELECT Borough, count(*) FROM j GROUP BY 1", {}),
    ("low-card key, bypass partial", "SELECT Borough, count(*) FROM j GROUP BY 1",
     {"spark.sql.execution.bypassPartialAggregation": "true"}),
    ("count(DISTINCT ts) by Borough", "SELECT Borough, count(DISTINCT tpep_pickup_datetime) FROM j GROUP BY 1", {}),
    ("approx_count_distinct(ts)", "SELECT Borough, approx_count_distinct(tpep_pickup_datetime) FROM j GROUP BY 1", {}),
    ("percentile(fare, 0.5)", "SELECT Borough, percentile(fare_amount, 0.5) FROM j GROUP BY 1", {}),
    ("percentile_approx(fare, 0.5)", "SELECT Borough, percentile_approx(fare_amount, 0.5) FROM j GROUP BY 1", {}),
    ("ROLLUP(Borough, payment_type)", "SELECT Borough, payment_type, count(*) FROM j GROUP BY ROLLUP(1, 2)", {}),
    ("UNION ALL of the 3 levels", """SELECT Borough, payment_type, count(*) FROM j GROUP BY 1, 2
       UNION ALL SELECT Borough, NULL, count(*) FROM j GROUP BY 1 UNION ALL SELECT NULL, NULL, count(*) FROM j""", {}),
    ("ROLLUP, from the Parquet file", "SELECT VendorID, payment_type, count(*) FROM trips GROUP BY ROLLUP(1, 2)", {}),
    ("UNION ALL, from the Parquet file", """SELECT VendorID, payment_type, count(*) FROM trips GROUP BY 1, 2
       UNION ALL SELECT VendorID, NULL, count(*) FROM trips GROUP BY 1
       UNION ALL SELECT NULL, NULL, count(*) FROM trips""", {}),
]
print("  distinct fare values:", spark.sql("SELECT count(DISTINCT fare_amount) FROM j").first()[0],
      "| distinct pickup times:", spark.sql("SELECT count(DISTINCT tpep_pickup_datetime) FROM j").first()[0])
for label, sql, conf in CASES:
    med, lo, hi = timed(sql, conf)
    print(f"  {label:<34} median {med:6.3f} s   (min {lo:.3f}, max {hi:.3f})")
