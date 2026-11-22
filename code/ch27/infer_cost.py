"""Chapter 27: what schema inference costs on a large JSON input (one million taxi trips as JSON Lines).
Medians of 5 runs after one warm-up. Run under the shared benchmark lock:

    flock $HOME/spark-masterclass/data/.bench.lock uv run python ch27/infer_cost.py
"""
import os
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, TRIPS, WORK

spark = session("ch27-infer-cost", spark_sql_session_timeZone="UTC")
src = WORK / "ch27" / "trips_json"
if not src.exists():
    (spark.read.parquet(str(TRIPS)).where(F.col("_metadata.row_index") < 1_000_000)
     .write.json(str(src)))
size = sum(f.stat().st_size for f in src.glob("part-*")) / 1e6
schema = spark.read.parquet(str(TRIPS)).schema


def median_s(fn, runs=5):
    fn()
    ts = []
    for _ in range(runs):
        t = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t)
    return statistics.median(ts)


print(f"input: 1,000,000 trips as JSON Lines, {size:.1f} MB, {os.cpu_count()} cores, local[*]")
rows = [
    ("spark.read.json(path)                     (inference only)",
     lambda: spark.read.json(str(src))),
    ("spark.read.option(samplingRatio=0.1).json  (inference only)",
     lambda: spark.read.option("samplingRatio", 0.1).json(str(src))),
    ("spark.read.json(path).count()",
     lambda: spark.read.json(str(src)).count()),
    ("spark.read.schema(schema).json(path).count()",
     lambda: spark.read.schema(schema).json(str(src)).count()),
    ("spark.read.json(path).agg(sum(fare_amount))",
     lambda: spark.read.json(str(src)).agg(F.sum("fare_amount")).collect()),
    ("spark.read.schema(schema).json.agg(sum(fare_amount))",
     lambda: spark.read.schema(schema).json(str(src)).agg(F.sum("fare_amount")).collect()),
]
for label, fn in rows:
    print(f"  {label:60} {median_s(fn):6.2f} s")
print("inferred == parquet types:",
      {f.name: f.dataType.simpleString() for f in spark.read.json(str(src)).schema.fields
       if f.dataType.simpleString() != schema[f.name].dataType.simpleString()})
