"""Chapter 9: describe, summary and the DataFrame.stat functions on the taxi month.

Works on Classic and on Connect (set SPARK_REMOTE). Timings are medians of 3 after a warmup.
Run from code/:  uv run python ch09/stats.py
"""
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS

spark = session("ch09-stats", spark_ui_showConsoleProgress="false")
trips = spark.read.parquet(str(TRIPS))
print("session:", type(spark).__module__)
num = trips.select("passenger_count", "trip_distance", "fare_amount", "tip_amount")


def timed(fn, n=3):
    fn()
    ts = []
    for _ in range(n):
        s = time.perf_counter(); out = fn(); ts.append(time.perf_counter() - s)
    return statistics.median(ts), out


print("\n== 1. describe(): count, mean, stddev, min, max")
num.describe().show()

print("== 2. summary(): describe plus the quartiles, or the statistics that you name")
num.summary().show()
trips.select("fare_amount", "payment_type").summary("count", "min", "1%", "50%", "99%", "max",
                                                  "count_distinct", "approx_count_distinct").show()
print("the plan of summary() is one aggregate over the data:")
print("\n".join(l for l in plan(num.summary(), "simple").splitlines()
                if "Aggregate" in l or "FileScan" in l)[:230] + " ...")

print("\n== 3. stat.corr and stat.cov (Pearson, two columns, one pass)")
st = trips.stat
print(f"corr(trip_distance, fare_amount) = {st.corr('trip_distance', 'fare_amount'):.4f}")
print(f"corr(fare_amount, tip_amount)    = {st.corr('fare_amount', 'tip_amount'):.4f}")
print(f"cov(fare_amount, tip_amount)     = {st.cov('fare_amount', 'tip_amount'):.2f}")
clean = trips.where("trip_distance BETWEEN 0.1 AND 100 AND fare_amount BETWEEN 1 AND 500")
print(f"corr(trip_distance, fare_amount) without outliers = {clean.stat.corr('trip_distance', 'fare_amount'):.4f}")

print("\n== 4. stat.crosstab: a contingency table of two columns")
st.crosstab("VendorID", "payment_type").orderBy("VendorID_payment_type").show()

print("== 5. stat.freqItems: items in more than a fraction of rows (approximate, may include false positives)")
print(st.freqItems(["PULocationID", "payment_type"], support=0.05).first().asDict())

print("\n== 6. stat.approxQuantile: the error you allow sets the cost")
print(f"  {'relativeError':>13} {'25% / 50% / 75% of fare_amount':>34} {'time':>7}")
for err in (0.0, 0.001, 0.01, 0.1):
    t, q = timed(lambda: st.approxQuantile("fare_amount", [0.25, 0.5, 0.75], err))
    print(f"  {err:>13} {str(q):>34} {t:6.2f}s")
print("  several columns at once:", st.approxQuantile(["fare_amount", "tip_amount"], [0.5, 0.99], 0.01))

print("\n== 7. stat.sampleBy: a stratified sample with one fraction per key")
fractions = {1: 0.01, 2: 0.01, 3: 0.5, 4: 0.5}
strat = st.sampleBy("payment_type", fractions, seed=42)
counts = {r.payment_type: r["count"] for r in trips.groupBy("payment_type").count().collect()}
for r in strat.groupBy("payment_type").count().orderBy("payment_type").collect():
    print(f"  payment_type {r.payment_type}: {r['count']:>6,} of {counts[r.payment_type]:>9,}"
          f"  ({r['count'] / counts[r.payment_type]:.3f}, asked {fractions.get(r.payment_type, 0)})")
print("  keys not in fractions get fraction 0:", strat.where("payment_type = 0").count(), "rows of type 0")
