"""Chapter 20: a skewed applyInPandas, and the salt that spreads its largest group.

Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch20/salting.py
"""
import statistics, sys, time, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import functions as F
from common.book import session, TRIPS

warnings.filterwarnings("ignore")
spark = session("ch20-salting", spark_ui_showConsoleProgress="false")
trips = spark.read.parquet(str(TRIPS)).select("PULocationID", "fare_amount", "tip_amount")
SALT = 16


def partial(pdf: pd.DataFrame) -> pd.DataFrame:
    """Sums that can be added again later, plus the size of the group that this call got."""
    return pd.DataFrame({"PULocationID": [pdf.PULocationID.iloc[0]], "rows": [len(pdf)],
                         "fares": [pdf.fare_amount.sum()], "tips": [pdf.tip_amount.sum()]})


schema = "PULocationID int, rows long, fares double, tips double"
plain = lambda: trips.groupBy("PULocationID").applyInPandas(partial, schema)
salted = lambda: (trips.withColumn("salt", F.floor(F.rand(7) * SALT))
                  .groupBy("PULocationID", "salt").applyInPandas(partial, schema)
                  .groupBy("PULocationID").agg(F.sum("rows").alias("rows"), F.sum("fares").alias("fares"),
                                               F.sum("tips").alias("tips")))
largest = lambda df: df.agg(F.max("rows")).first()[0]
print(f"largest group that one Python call gets: plain {largest(plain()):,}; "
      f"salted ({SALT} salts) {largest(trips.withColumn('salt', F.floor(F.rand(7) * SALT)).groupBy('PULocationID', 'salt').applyInPandas(partial, schema)):,}")

a = {r.PULocationID: (r.rows, round(r.fares, 2)) for r in plain().collect()}
b = {r.PULocationID: (r.rows, round(r.fares, 2)) for r in salted().collect()}
print("same totals per zone:", a == b)


def median_s(build, runs=5):
    build().write.format("noop").mode("overwrite").save()
    ts = []
    for _ in range(runs):
        t = time.perf_counter(); build().write.format("noop").mode("overwrite").save()
        ts.append(time.perf_counter() - t)
    return statistics.median(ts)


print(f"noop write, median of 5 after a warm-up: plain {median_s(plain):.2f} s, salted {median_s(salted):.2f} s")
