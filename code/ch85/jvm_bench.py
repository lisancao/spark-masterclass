"""Chapter 85, Step 12: the JVM function options against the native expression. Timed runs.

Run from code/:  flock $HOME/spark-masterclass/data/.bench.lock uv run python ch85/jvm_bench.py
Each time is the median of 5 runs after 1 warm-up run, with a new DataFrame for every run.
"""
import os
import platform
import statistics
import sys
import time
import warnings
from pathlib import Path

import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import BooleanType  # noqa: E402
from common.book import session, TRIPS, ROOT  # noqa: E402

warnings.filterwarnings("ignore")
HERE = Path(__file__).resolve().parent
JARS = f"{HERE / 'jfun' / 'target' / 'jfun.jar'},{HERE / 'jext' / 'target' / 'jext-0.1.0.jar'}"
META = ROOT / "data" / "work" / "ch85-metastore"
SIG = b"\x89PNG\r\n\x1a\n"
N_BLOBS = 20_000_000
spark = session("ch85-jvm-bench", spark_jars=JARS, spark_sql_extensions="jext.Extensions",
                spark_ui_showConsoleProgress="false",
                **{"spark.sql.catalogImplementation": "hive", "spark.sql.warehouse.dir": str(META / "warehouse"),
                   "spark.hadoop.javax.jdo.option.ConnectionURL":
                       f"jdbc:derby:;databaseName={META / 'metastore_db'};create=true"})
spark.sql("CREATE TEMPORARY FUNCTION is_png_hive AS 'jfun.IsPngHive'")
spark.sql("CREATE TEMPORARY FUNCTION hive_avg AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage'")
spark.udf.registerJavaFunction("is_png_udf", "jfun.IsPngUdf", BooleanType())
spark.udf.registerJavaUDAF("mean_fare_udaf", "jfun.MeanFareUdaf")
spark._jvm.jfun.Register.all(spark._jsparkSession)
print(f"local[*] on {os.cpu_count()} cores, {platform.machine()}, Spark {spark.version}, "
      f"Python {platform.python_version()}")


def timed(run):
    times = []
    for i in range(6):
        t = time.perf_counter()
        n = run()
        if i:
            times.append(time.perf_counter() - t)
    return statistics.median(times), n, min(times), max(times)


def table(variants):
    rows = [(name, *timed(r)) for name, r in variants]
    base = rows[0][1]
    for name, s, n, lo, hi in rows:
        print(f"  {name:44} {s:6.3f} s  {s / base:4.1f}x  [{lo:.3f} to {hi:.3f}]  ({n:,} rows)")


# ---- A: scalar, is_png over 20,000,000 cached binary values (as in bench.py)
blobs = (spark.range(N_BLOBS)
         .select(F.when(F.col("id") % 3 == 0, F.lit(SIG + b"\x00\x00\x00\rIHDR"))
                  .otherwise(F.lit(b"GIF89a\x01\x00\x01\x00\x80\x00")).alias("b"))
         .cache())
blobs.count()
print(f"\nA. is_png over {N_BLOBS:,} cached values (filter, count)")
table([
    ("native is_png_fast (StaticInvoke)", lambda: blobs.filter(F.call_function("is_png_fast", "b")).count()),
    ("built-in substring(b, 1, 8) = X'89504E47...'", lambda: blobs.filter(F.substring("b", 1, 8) == F.lit(SIG)).count()),
    ("Java UDF1 (registerJavaFunction)", lambda: blobs.filter(F.expr("is_png_udf(b)")).count()),
    ("Hive GenericUDF", lambda: blobs.filter(F.expr("is_png_hive(b)")).count()),
])

# ---- B: aggregate, the mean fare per pickup zone over the June trips
trips = spark.read.parquet(str(TRIPS)).select("PULocationID", "fare_amount").cache()
n = trips.count()


@F.pandas_udf("double")
def mean_pandas(v: pd.Series) -> float:
    return v.mean()


def agg(col):
    return lambda: len(trips.groupBy("PULocationID").agg(col.alias("m")).collect())   # collect: keep the aggregate


print(f"\nB. mean fare per pickup zone over {n:,} cached trips (group by, collect)")
table([
    ("built-in avg", agg(F.avg("fare_amount"))),
    ("Aggregator with functions.udaf", agg(F.expr("mean_fare(fare_amount)"))),
    ("UserDefinedAggregateFunction (deprecated)", agg(F.expr("mean_fare_udaf(fare_amount)"))),
    ("Hive GenericUDAFAverage", agg(F.expr("hive_avg(fare_amount)"))),
    ("pandas UDF (grouped aggregate)", agg(mean_pandas("fare_amount"))),
])
spark.stop()
