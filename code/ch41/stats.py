"""Chapter 41: table and column statistics, what ANALYZE TABLE stores, and how the estimates reach the plan.

Run from code/:  uv run python ch41/stats.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from c86 import start, trips_clean, zones, plan, F

spark = start("ch41-stats")
print(f"Spark {spark.version}")


def section(t):
    print(f"\n=== {t}")


def stats_row(table):
    rows = spark.sql(f"DESC EXTENDED {table}").collect()
    s = [r.data_type for r in rows if r.col_name == "Statistics"]
    return s[0] if s else "(no Statistics row)"


def cost_line(df, node):
    """The first line of the cost plan for a node, e.g. 'Filter (...), Statistics(sizeInBytes=..., rowCount=...)'."""
    for line in plan(df, "cost").splitlines():
        if node in line and "Statistics(" in line:
            st = line[line.index("Statistics("):]
            return re.sub(r"\s+", " ", st)
    return "?"


# ---- the tables: trips with a key, and a second table of fares for the same trips
spark.sql("DROP TABLE IF EXISTS trips"); spark.sql("DROP TABLE IF EXISTS fares"); spark.sql("DROP TABLE IF EXISTS zones")
t = trips_clean(spark).withColumn("trip_id", F.monotonically_increasing_id())
t.select("trip_id", "VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID", "passenger_count",
         "trip_distance").write.saveAsTable("trips")
spark.table("trips").select("trip_id").limit(0).count()
t.select("trip_id", "payment_type", "fare_amount", "tip_amount", "total_amount").write.saveAsTable("fares")
zones(spark).write.saveAsTable("zones")

section("1. what the catalog knows before ANALYZE")
for tb in ("trips", "fares", "zones"):
    print(f"  {tb:6} Statistics: {stats_row(tb)}")
q = spark.table("fares").where("total_amount > 500")
print("  cost of fares WHERE total_amount > 500:")
print(f"    Relation: {cost_line(q, 'Relation')}")
print(f"    Filter:   {cost_line(q, 'Filter')}")

section("2. ANALYZE TABLE, three forms")
spark.sql("ANALYZE TABLE fares COMPUTE STATISTICS NOSCAN")
print(f"  NOSCAN              -> {stats_row('fares')}")
spark.sql("ANALYZE TABLE fares COMPUTE STATISTICS")
print(f"  COMPUTE STATISTICS  -> {stats_row('fares')}")
spark.sql("ANALYZE TABLE fares COMPUTE STATISTICS FOR COLUMNS total_amount, payment_type")
print(f"  FOR COLUMNS         -> {stats_row('fares')}")
for col in ("total_amount", "payment_type"):
    print(f"  DESC EXTENDED fares {col}:")
    for r in spark.sql(f"DESC EXTENDED fares {col}").collect():
        if r.info_name not in ("col_name", "comment"):
            print(f"    {r.info_name:15} {r.info_value}")

section("3. the same filter, CBO off and on")
for cbo in ("false", "true"):
    spark.conf.set("spark.sql.cbo.enabled", cbo)
    q = spark.table("fares").where("total_amount > 500")
    print(f"  spark.sql.cbo.enabled={cbo}")
    print(f"    Relation: {cost_line(q, 'Relation')}")
    print(f"    Filter:   {cost_line(q, 'Filter')}")

section("4. estimated rows against real rows, without and with a histogram")
preds = ["total_amount > 500", "total_amount > 100", "total_amount < 10", "total_amount BETWEEN 15 AND 25",
         "payment_type = 2", "payment_type = 4"]
actual = {p: spark.table("fares").where(p).count() for p in preds}


def est_rows(p):
    s = cost_line(spark.table("fares").where(p), "Filter")
    m = re.search(r"rowCount=([0-9.E+]+)", s)
    return int(float(m.group(1))) if m else None


uniform = {p: est_rows(p) for p in preds}
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
spark.sql("ANALYZE TABLE fares COMPUTE STATISTICS FOR COLUMNS total_amount, payment_type")
hist = {p: est_rows(p) for p in preds}
print(f"  {'predicate':32} {'real rows':>11} {'no histogram':>13} {'histogram':>11}")
for p in preds:
    print(f"  {p:32} {actual[p]:>11,} {uniform[p]:>13,} {hist[p]:>11,}")
h = [r for r in spark.sql("DESC EXTENDED fares total_amount").collect() if r.info_name.startswith("bin_") or r.info_name == "histogram"]
print(f"  histogram of total_amount: {h[0].info_value}")
for r in h[1:4] + h[-2:]:
    print(f"    {r.info_name:8} {r.info_value}")

section("5. ANALYZE for every column, and for every table in a database")
spark.sql("ANALYZE TABLE trips COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLES IN default COMPUTE STATISTICS")
for tb in ("trips", "fares", "zones"):
    print(f"  {tb:6} Statistics: {stats_row(tb)}")
cols = {r.info_name: r.info_value for r in spark.sql("DESC EXTENDED trips trip_distance").collect()}
print("  trips.trip_distance: " + ", ".join(f"{k} {cols[k]}" for k in ("min", "max", "num_nulls", "distinct_count")))
