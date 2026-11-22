"""Chapter 3: what Tungsten keeps in memory, read from the JVM.

1. One row of the taxi data as an UnsafeRow: the null bitset, the fixed 8-byte slots, and
   the variable-length region, printed as hex words.
2. The same rows cached three ways (a Python RDD of tuples, a DataFrame in the in-memory
   columnar cache, and the Parquet file on disk), sizes from the Storage endpoint.

Classic only (it reaches into the JVM through py4j). Run from code/:  uv run python ch03/tungsten.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, ui_api, TRIPS

spark = session("ch03-tungsten", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
trips = spark.read.parquet(str(TRIPS))

one = trips.select("VendorID", "trip_distance", "store_and_fwd_flag", "fare_amount").limit(1)
print(one.collect()[0])
row = one._jdf.queryExecution().toRdd().first()          # an InternalRow, from the JVM side
print(f"class: {row.getClass().getName()}")
print(f"numFields={row.numFields()}  sizeInBytes={row.getSizeInBytes()}")
words = str(row.toString()).strip("[]").split(",")
labels = ["null bitset"] + [f"field {i}" for i in range(row.numFields())]
print("8-byte words, as UnsafeRow.toString() prints them (hex):")
for i, word in enumerate(words):
    label = labels[i] if i < len(labels) else "variable-length data"
    print(f"  word {i}: {word:>18}   {label}")

cols = ["tpep_pickup_datetime", "PULocationID", "trip_distance", "fare_amount", "total_amount"]
df = trips.select(cols)
py = df.rdd.map(tuple).setName("python rdd of tuples").cache()
sc.setJobDescription("cache python rdd")
py.count()
df.cache()
sc.setJobDescription("cache dataframe")
df.count()
parquet = sum(p.stat().st_size for p in TRIPS.glob("*.parquet"))
print(f"\n{df.count():,} rows x {len(cols)} columns, cached two ways (REST: /storage/rdd):")
for r in ui_api("storage/rdd"):
    name = "python rdd of tuples" if r["name"].startswith("python") else "dataframe cache (columnar)"
    print(f"  {name:<28} {r['memoryUsed'] / 2**20:8.1f} MiB  ({r['storageLevel']})")
print(f"  {'parquet file, all 21 columns':<28} {parquet / 2**20:8.1f} MiB  (on disk, compressed)")

import statistics
import time


def median_s(fn, runs=5):
    fn()
    ts = []
    for _ in range(runs):
        t = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t)
    return statistics.median(ts)


py_s = median_s(lambda: py.map(lambda t: t[3]).sum())
df_s = median_s(lambda: df.agg({"fare_amount": "sum"}).collect())
print(f"\nsum(fare_amount) over each cache, median of 5 runs:")
print(f"  python rdd of tuples       {py_s:6.2f} s")
print(f"  dataframe cache            {df_s:6.2f} s   ({py_s / df_s:.0f}x faster)")
