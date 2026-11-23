"""Chapter 3: the same question as RDD code and as a DataFrame, measured, with what each one tells Spark.

The question: for June 2026, the number of trips and the average fare for each pickup hour.
  rdd         Python functions over Row objects: Spark sees opaque lambdas
  dataframe   the same logic as expressions: Catalyst, Tungsten and whole-stage codegen apply
  df, no wsc  the DataFrame with spark.sql.codegen.wholeStage=false

Medians of 5 runs after one warmup run of each. Classic only (RDDs are not in Spark Connect).
Run from code/:  uv run python ch03/rdd_vs_df.py
"""
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS

spark = session("ch03-rdd-vs-df", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
RUNS = 5
trips = spark.read.parquet(str(TRIPS))


def with_rdd():
    return (trips.rdd                                                       # (1) Row objects
            .filter(lambda r: r.fare_amount is not None and r.fare_amount > 0)
            .map(lambda r: (r.tpep_pickup_datetime.hour, (r.fare_amount, 1)))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
            .mapValues(lambda s: (s[1], round(s[0] / s[1], 2)))
            .sortByKey())


def with_df():
    return (trips.where(F.col("fare_amount") > 0)
            .groupBy(F.hour("tpep_pickup_datetime").alias("hour"))
            .agg(F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
            .orderBy("hour"))


def timed(fn):
    fn()                                      # warmup
    ts = []
    for _ in range(RUNS):
        t = time.perf_counter()
        out = fn()
        ts.append(time.perf_counter() - t)
    return statistics.median(ts), out


rdd_s, rdd_rows = timed(lambda: with_rdd().collect())
df_s, df_rows = timed(lambda: with_df().collect())
spark.conf.set("spark.sql.codegen.wholeStage", "false")
nowsc_s, _ = timed(lambda: with_df().collect())
spark.conf.set("spark.sql.codegen.wholeStage", "true")

same = [(h, (n, f)) for h, n, f in df_rows] == rdd_rows
print(f"same answer from both: {same}   hour 18: {rdd_rows[18]}")
print(f"\nmedian of {RUNS} runs over {trips.count():,} rows:")
print(f"  rdd (Python lambdas)          {rdd_s:6.2f} s")
print(f"  dataframe                     {df_s:6.2f} s   {rdd_s / df_s:5.1f}x faster than rdd")
print(f"  dataframe, wholeStage=false   {nowsc_s:6.2f} s   {nowsc_s / df_s:5.1f}x slower than with codegen")

# A CPU-bound query, where the time goes to expression evaluation instead of the scan.
cpu = lambda: spark.range(0, 400_000_000).where("id % 7 = 3").selectExpr("sum(id * 2 + 1)").collect()
on_s, _ = timed(cpu)
spark.conf.set("spark.sql.codegen.wholeStage", "false")
off_s, _ = timed(cpu)
spark.conf.set("spark.sql.codegen.wholeStage", "true")
print(f"\nrange(400M), filter, sum:  codegen on {on_s:5.2f} s   off {off_s:5.2f} s   "
      f"{off_s / on_s:4.1f}x slower without it")

print("\nwhat Spark knows about the RDD version (toDebugString):")
print(with_rdd().toDebugString().decode().replace(str(Path(__file__).resolve().parent) + "/", ""))

print("what Spark knows about the DataFrame version (explain extended):")
print(plan(with_df(), "extended"))
