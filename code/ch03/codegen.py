"""Chapter 3: the Java code that whole-stage codegen writes, and the plan that AQE settles on.

Run from code/:  uv run python ch03/codegen.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS

spark = session("ch03-codegen", spark_ui_showConsoleProgress="false")
trips = spark.read.parquet(str(TRIPS))
q = (trips.where(F.col("fare_amount") > 0)
     .groupBy(F.hour("tpep_pickup_datetime").alias("hour"))
     .agg(F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
     .orderBy("hour"))

print("before the run:", plan(q, "codegen").splitlines()[0])
q.collect()
code = plan(q, "codegen")
lines = code.splitlines()
print("after the run: ", lines[0])
for i, line in enumerate(lines):
    if line.startswith("== Subtree"):
        print(line[:110])
        j = i + 1
        while j < len(lines) and not lines[j].startswith("Generated code"):
            print("   ", lines[j][:106])
            j += 1
first = code.index("Generated code")
body = code[first:].splitlines()
print(f"\nsubtree 1 generated code: {next(i for i, l in enumerate(body[1:], 1) if l.startswith('== Subtree') or l.startswith('Generated code')) - 1} lines; the loop that fuses scan, filter, project and partial aggregate:")
start = next(i for i, l in enumerate(body) if "hashAgg_doAggregateWithKeys_0(int partitionIndex)" in l)
for l in body[start:start + 19]:
    print(l[:118])

print("\nthe final plan after AQE (explain after the run):")
print("\n".join(l[:110] for l in plan(q, "simple").splitlines()[:14]))
