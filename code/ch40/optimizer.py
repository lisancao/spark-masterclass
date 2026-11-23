"""Chapter 40: what the optimizer did to one query, and which rules did it.

Classic only: it reads the JVM's QueryPlanningTracker through df._jdf.
Run from code/:  uv run python ch40/optimizer.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, TRIPS, ZONES

spark = session("ch40-optimizer", spark_sql_session_timeZone="UTC")

trips = spark.read.parquet(str(TRIPS))
zones = spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))

q = (trips.join(zones, trips.PULocationID == zones.LocationID)
     .where((F.col("Borough") == "Queens") & (F.col("PULocationID") < 140))
     .select("Zone", (F.col("fare_amount") * (F.lit(1) + F.lit(0.5) - F.lit(0.5))).alias("fare"))
     .groupBy("Zone").agg(F.round(F.sum("fare")).alias("fares")))


def short(text, width=150):
    return "\n".join(l if len(l) <= width else l[:width] + " ..." for l in text.splitlines())


qe = q._jdf.queryExecution()
print("== Analyzed Logical Plan ==")
print(short(qe.analyzed().toString()))
print("== Optimized Logical Plan ==")
print(short(qe.optimizedPlan().toString()))
print("== Physical Plan ==")
print(short(qe.executedPlan().toString()))

# Every rule that ran, from the tracker the query carries. "Effective" means the rule changed the plan.
rules = []
it = qe.tracker().rules().iterator()
while it.hasNext():
    t = it.next()
    s = t._2()
    rules.append((t._1().split(".")[-1], s.numInvocations(), s.numEffectiveInvocations(), s.totalTimeNs()))
print(f"\n{len(rules)} rules ran; {sum(r[2] > 0 for r in rules)} of them changed the plan:\n")
print(f"  {'rule':<48}{'runs':>6}{'changed':>9}")
for name, runs, eff, ns in sorted(rules, key=lambda r: r[0]):
    if eff:
        print(f"  {name:<48}{runs:>6}{eff:>9}")

phases = qe.tracker().phases()
print("\nPlanning phases:", ", ".join(
    f"{k} {phases.apply(k).durationMs()} ms" for k in ("analysis", "optimization", "planning")))
