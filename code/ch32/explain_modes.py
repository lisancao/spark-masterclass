"""Chapter 32: the five explain modes, and the four plans behind every query.

Run from code/:  uv run python ch32/explain_modes.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS, MONTH_START, MONTH_END

spark = session("ch32-explain", spark_sql_session_timeZone="UTC")

trips = spark.read.parquet(str(TRIPS))
q = (trips
     .where((F.col("tpep_pickup_datetime") >= MONTH_START) &
            (F.col("tpep_pickup_datetime") < MONTH_END) &
            (F.col("passenger_count") == 1))
     .groupBy("PULocationID")
     .agg(F.count("*").alias("trips"), F.avg("fare_amount").alias("avg_fare")))


def section(title):
    print(f"\n######## {title}\n")


section("explain('extended')")
print(plan(q, "extended"))

section("explain('formatted')")
print(plan(q, "formatted"))

section("explain('cost'): the optimized plan with size estimates")
print(plan(q, "cost"))

section("explain('codegen') before the query runs")
print(plan(q, "codegen").strip())

rows = q.collect()
section(f"explain() after collect() ({len(rows)} rows): the final plan")
print(plan(q, "simple"))

section("explain('codegen') after collect(): the start of one generated class")
code = plan(q, "codegen").splitlines()
print("\n".join(code[:4]))
start = next(i for i, l in enumerate(code) if "processNext" in l)
print("...")
print("\n".join(code[start:start + 24]))
print(f"... ({len(code)} lines in all)")

section("a mode that does not exist")
try:
    q.explain("bogus")
except Exception as e:
    print(f"{type(e).__name__}: {e}")
