"""Chapter 72: Arrow at the boundary between the JVM and Python, measured with toPandas().

The same 1,000,000 rows x 5 columns go to pandas with spark.sql.execution.arrow.pyspark.enabled
off (rows pickled one by one) and on (Arrow record batches). Medians of 5 runs after a warmup.
Run from code/:  uv run python ch72/arrow.py
"""
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, TRIPS

spark = session("ch72-arrow", spark_ui_showConsoleProgress="false")
df = (spark.read.parquet(str(TRIPS))
      .select("tpep_pickup_datetime", "PULocationID", "trip_distance", "fare_amount", "total_amount")
      .limit(1_000_000).cache())
df.count()


def median_s(runs=5):
    df.toPandas()
    ts = []
    for _ in range(runs):
        t = time.perf_counter()
        pdf = df.toPandas()
        ts.append(time.perf_counter() - t)
    return statistics.median(ts), pdf


KEY = "spark.sql.execution.arrow.pyspark.enabled"
for on in ("false", "true"):
    spark.conf.set(KEY, on)
    s, pdf = median_s()
    print(f"{KEY}={on:<5}  toPandas() of {len(pdf):,} rows: {s:5.2f} s")
