"""Chapter 20: one fare-band rule, run six ways over the June trips, timed.

Each variant computes the band of every trip and writes the result to the noop sink.
Median of RUNS runs after one warm-up. Classic (transpilation is a classic-only feature in 4.3).
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch20/bench.py
"""
import os, statistics, sys, time, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import functions as F
from common.book import session, plan, TRIPS

warnings.filterwarnings("ignore")
spark = session("ch20-bench", spark_ui_showConsoleProgress="false")
RUNS = int(os.environ.get("RUNS", 5))
trips = spark.read.parquet(str(TRIPS))


def band(fare):
    if fare is None or fare <= 0:
        return "invalid"
    else:
        return "under $10" if fare < 10 else "$10 to $30" if fare < 30 else "over $30"


def builtin(c):
    return (F.when(c.isNull() | (c <= 0), "invalid").when(c < 10, "under $10")
            .when(c < 30, "$10 to $30").otherwise("over $30"))


@F.pandas_udf("string")
def band_pandas(fare: pd.Series) -> pd.Series:
    out = pd.Series("over $30", index=fare.index)
    out[fare < 30] = "$10 to $30"
    out[fare < 10] = "under $10"
    out[fare.isna() | (fare <= 0)] = "invalid"
    return out


band_arrow = F.udf(band, "string")                     # Arrow by default in 4.3
band_pickled = F.udf(band, "string", useArrow=False)   # rows pickled to the worker
spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "true")
band_transpiled = F.udf(band, "string", useArrow=False)   # read and lowered when it is created
spark.conf.unset("spark.sql.experimental.optimizer.transpilePyUDFs")

VARIANTS = [
    ("built-in when()", lambda c: builtin(c)),
    ("@udf, transpiled", lambda c: band_transpiled(c)),
    ("@pandas_udf", lambda c: band_pandas(c)),
    ("@udf, Arrow", lambda c: band_arrow(c)),
    ("@udf, useArrow=False", lambda c: band_pickled(c)),
]
python_nodes = ("ArrowEvalPython", "BatchEvalPython")
print(f"{trips.count():,} trips, {os.cpu_count()} cores; noop write; median of {RUNS} runs after a warm-up\n")
print(f"{'variant':<24}{'Python node in the plan':<26}{'seconds':>8}{'vs built-in':>13}")
base = None
results = {}
for name, f in VARIANTS:
    df = trips.select(f(F.col("fare_amount")).alias("band"))
    node = next((n for n in python_nodes if n in plan(df, "simple")), "none")
    df.write.format("noop").mode("overwrite").save()
    ts = []
    for _ in range(RUNS):
        t = time.perf_counter()
        trips.select(f(F.col("fare_amount")).alias("band")).write.format("noop").mode("overwrite").save()
        ts.append(time.perf_counter() - t)
    m = statistics.median(ts)
    base = base or m
    print(f"{name:<24}{node:<26}{m:>8.2f}{m / base:>12.1f}x", flush=True)
    results[name] = sorted(tuple(r) for r in trips.groupBy(f(F.col("fare_amount"))).count().collect())

same = all(v == results["built-in when()"] for v in results.values())
print(f"\nall variants give the same bands and counts: {same}")
