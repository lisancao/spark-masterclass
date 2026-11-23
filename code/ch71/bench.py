"""Chapter 71: how much a native function, a rule and the 4.3 transpiler save. Timed runs.

Run from code/:  flock $HOME/spark-masterclass/data/.bench.lock uv run python ch71/bench.py
Each time is the median of 5 runs after 1 warm-up run, with a new DataFrame for every run.
"""
import os, platform, statistics, sys, time, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, TRIPS  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

warnings.filterwarnings("ignore")
JAR = Path(__file__).resolve().parent / "jext" / "target" / "jext-0.1.0.jar"
SIG = b"\x89PNG\r\n\x1a\n"
N_BLOBS = 20_000_000
spark = session("ch71-bench", spark_jars=str(JAR), spark_sql_extensions="jext.Extensions")
print(f"local[*] on {os.cpu_count()} cores, {platform.processor() or platform.machine()},"
      f" Spark {spark.version}, Python {platform.python_version()}")


def timed(build):
    """build() returns a new DataFrame; time its count(). Median of 5 after a warm-up."""
    times = []
    for i in range(6):
        t = time.perf_counter()
        n = build().count()
        if i:
            times.append(time.perf_counter() - t)
    return statistics.median(times), n


def python_node(df):
    p = df._jdf.queryExecution().executedPlan().toString()
    return next((l.strip(" +-").split(" [")[0] for l in p.splitlines() if "EvalPython" in l), "no Python")


def table(variants):
    rows = [(name, *timed(b), python_node(b())) for name, b in variants]
    base = rows[0][1]
    for name, s, n, node in rows:
        print(f"  {name:44} {s:7.3f} s  {s / base:5.1f}x  {node:16} ({n:,} rows)")


# ---- A: is_png over N_BLOBS cached binary values, a third of them PNG signatures
blobs = (spark.range(N_BLOBS)
         .select(F.when(F.col("id") % 3 == 0, F.lit(SIG + b"\x00\x00\x00\rIHDR"))
                  .otherwise(F.lit(b"GIF89a\x01\x00\x01\x00\x80\x00")).alias("b"))
         .cache())
blobs.count()


@F.udf("boolean")
def is_png(b):                     # the rule rewrites this one (name is_png)
    return b is not None and bytes(b[:8]) == SIG


@F.udf("boolean")
def png_check(b):                  # the same body: stays in Python (Arrow, the 4.3 default)
    return b is not None and bytes(b[:8]) == SIG


@F.pandas_udf("boolean")
def png_pandas(s):
    return s.map(lambda b: b is not None and bytes(b[:8]) == SIG)


variants = [
    ("is_png_fast (StaticInvoke, in codegen)", lambda: blobs.filter(F.call_function("is_png_fast", "b"))),
    ("Python UDF is_png, rewritten by PngRule", lambda: blobs.filter(is_png("b"))),
    ("is_png (CodegenFallback)", lambda: blobs.filter(F.call_function("is_png", "b"))),
    ("built-in substring(b, 1, 8) = X'89504E47...'",
     lambda: blobs.filter(F.substring("b", 1, 8) == F.lit(SIG))),
    ("pandas UDF", lambda: blobs.filter(png_pandas("b"))),
    ("Python UDF (Arrow, the 4.3 default)", lambda: blobs.filter(png_check("b"))),
]
print(f"\nA. is_png over {N_BLOBS:,} cached values")
table(variants)

# ---- B: fare_band over the June 2026 taxi trips
trips = spark.read.parquet(str(TRIPS))
FLAG = "spark.sql.experimental.optimizer.transpilePyUDFs"


def band(fare: float):
    return ("invalid" if fare is None or fare <= 0
            else "under $10" if fare < 10 else "$10 to $30" if fare < 30 else "over $30")


# The flag is read twice: when F.udf builds the UDF, and when the optimizer runs.
# Build the pickled UDF with the flag off, the transpiled one with it on, and time with it on.
band_pickled = F.udf(band, "string", useArrow=False)
spark.conf.set(FLAG, "true")
band_transpiled = F.udf(band, "string", useArrow=False)
band_arrow = F.udf(band, "string")
f = F.col("fare_amount")
band_builtin = (F.when(f.isNull() | (f <= 0), "invalid").when(f < 10, "under $10")
                .when(f < 30, "$10 to $30").otherwise("over $30"))

variants = [
    ("built-in when(...) chain", lambda: trips.groupBy(band_builtin.alias("band")).count()),
    ("Python UDF, transpiled (4.3, useArrow=False)", lambda: trips.groupBy(band_transpiled("fare_amount").alias("band")).count()),
    ("Python UDF (Arrow, the 4.3 default)", lambda: trips.groupBy(band_arrow("fare_amount").alias("band")).count()),
    ("Python UDF (pickled, useArrow=False)", lambda: trips.groupBy(band_pickled("fare_amount").alias("band")).count()),
]
n_trips = trips.count()
print(f"\nB. fare_band over {n_trips:,} trips (group by band, count)")
table(variants)
spark.stop()
