"""Chapter 86: dynamic partition pruning. A filter on a small dimension prunes the partitions of the fact table.

Run from code/:  uv run python ch86/dpp.py   (timings under the bench lock)
"""
import datetime as dt
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from c86 import start, trips_clean, plan, median_s, noop, last_sql_metrics, TAXI, F

spark = start("ch86-dpp", spark_driver_memory="8g")
tables = {r.tableName for r in spark.sql("SHOW TABLES").collect()}
if "trips_by_day" not in tables:
    (trips_clean(spark, TAXI / "yellow-h1").withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
     .repartition("pickup_date").write.partitionBy("pickup_date").saveAsTable("trips_by_day"))
    HOLIDAYS = {"2026-01-01": "New Year's Day", "2026-01-19": "Martin Luther King Jr. Day",
                "2026-02-16": "Presidents' Day", "2026-05-25": "Memorial Day", "2026-06-19": "Juneteenth"}
    rows = []
    d = dt.date(2026, 1, 1)
    while d < dt.date(2026, 7, 1):
        rows.append((d, d.strftime("%A"), HOLIDAYS.get(d.isoformat())))
        d += dt.timedelta(days=1)
    spark.createDataFrame(rows, "pickup_date date, weekday string, holiday string").write.saveAsTable("days")
    spark.sql("ANALYZE TABLE days COMPUTE STATISTICS")

parts = spark.sql("SHOW PARTITIONS trips_by_day").count()
print(f"trips_by_day: {spark.table('trips_by_day').count():,} rows in {parts} partitions; days: {spark.table('days').count()} rows")

SQL = """SELECT d.weekday, t.PULocationID, count(*) AS trips, round(avg(t.tip_amount), 2) AS avg_tip,
                avg(t.trip_distance) AS avg_distance, sum(t.total_amount) AS amount,
                approx_count_distinct(t.DOLocationID) AS destinations
         FROM trips_by_day t JOIN days d ON t.pickup_date = d.pickup_date
         WHERE d.weekday = 'Sunday'
         GROUP BY d.weekday, t.PULocationID"""
HOLIDAY = """SELECT d.holiday, count(*) AS trips, round(avg(t.tip_amount), 2) AS avg_tip
         FROM trips_by_day t JOIN days d ON t.pickup_date = d.pickup_date
         WHERE d.holiday IS NOT NULL
         GROUP BY d.holiday ORDER BY d.holiday"""


def clean(l):
    l = re.sub(r"Location: InMemoryFileIndex\[[^\]]*\]", "Location: ...", l.strip())
    return re.sub(r"#\d+", "", l)


def scan_filters(q):
    p = plan(q, "formatted")
    pf = re.search(r"PartitionFilters: \[([^\]]*\]?)\]", p)
    return clean(pf.group(1)) if pf else ""


def scan_metrics():
    m = [mm for name, mm in last_sql_metrics(spark, "Scan parquet") if "number of partitions read" in mm]
    return m[0] if m else {}


def run(sql, label):
    t = median_s(lambda: spark.sql(sql).collect(), runs=7)
    spark.sql(sql).collect()
    m = scan_metrics()
    print(f"  {label:24} partitions read {m.get('number of partitions read', '?'):>4}  files {m.get('number of files read', '?'):>4}"
          f"  rows out of scan {m.get('number of output rows', '?'):>11}  {t:.2f} s")
    return t


print("\n=== 1. the plan for Sundays only: a subquery on the dimension filters the fact scan")
q = spark.sql(SQL)
p = plan(q, "formatted")
for l in p.splitlines():
    if any(k in l for k in ("PartitionFilters", "SubqueryBroadcast", "BroadcastExchange (", "Subquery:", "ReusedExchange")):
        print("  " + clean(l)[:140])
res_rows = q.collect()
print(f"  result: {len(res_rows)} pickup zones, {sum(r.trips for r in res_rows):,} Sunday trips")

print("\n=== 2. what the fact scan read, and the time (median of 7 after a warmup)")
res = {}
res["on"] = run(SQL, "DPP on (default)")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
res["off"] = run(SQL, "DPP off")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
print(f"  {'':24} no broadcast: PartitionFilters [{scan_filters(spark.sql(SQL))[:90]}]")
res["nob"] = run(SQL, "DPP on, broadcast off")
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
print(f"  DPP off / DPP on: {res['off'] / res['on']:.1f}x")

print("\n=== 3. holidays: 'holiday IS NOT NULL' is not a selective predicate")
print(f"  PartitionFilters: [{scan_filters(spark.sql(HOLIDAY))[:110]}]")
run(HOLIDAY, "no column statistics")
spark.sql("ANALYZE TABLE days COMPUTE STATISTICS FOR COLUMNS pickup_date, holiday")
spark.sql("ANALYZE TABLE trips_by_day COMPUTE STATISTICS FOR COLUMNS pickup_date")
spark.conf.set("spark.sql.cbo.enabled", "true")
print(f"  after ANALYZE ... FOR COLUMNS on both tables, and spark.sql.cbo.enabled=true:")
print(f"  PartitionFilters: [{scan_filters(spark.sql(HOLIDAY))[:110]}]")
run(HOLIDAY, "statistics and CBO")
spark.conf.set("spark.sql.cbo.enabled", "false")
H2 = HOLIDAY.replace("d.holiday IS NOT NULL", "d.holiday <> ''")
print(f"  the same filter written as \"d.holiday <> ''\", a comparison:")
print(f"  PartitionFilters: [{scan_filters(spark.sql(H2))[:110]}]")
run(H2, "holiday <> ''")
for r in spark.sql(H2).collect():
    print(f"    {r.holiday:28} {r.trips:>8,}  {r.avg_tip}")

print("\n=== 4. static partition pruning: the five holidays as literals")
static = spark.sql("""SELECT count(*) FROM trips_by_day WHERE pickup_date IN
   (DATE'2026-01-01', DATE'2026-01-19', DATE'2026-02-16', DATE'2026-05-25', DATE'2026-06-19')""")
print(f"  PartitionFilters: [{scan_filters(static)[:120]}]")
static.collect()
m = scan_metrics()
print(f"  partitions read {m.get('number of partitions read', '?')}, files read {m.get('number of files read', '?')}")

print("\n=== 5. new in 4.3: a small local DataFrame as the filtering side, with no filter at all (SPARK-54593)")
hol = spark.createDataFrame([(dt.date(2026, 1, 1),), (dt.date(2026, 1, 19),), (dt.date(2026, 2, 16),),
                             (dt.date(2026, 5, 25),), (dt.date(2026, 6, 19),)], "pickup_date date")
for cbo in ("false", "true"):
    spark.conf.set("spark.sql.cbo.enabled", cbo)
    q = spark.table("trips_by_day").join(hol, "pickup_date").groupBy("pickup_date").count()
    print(f"  spark.sql.cbo.enabled={cbo}: PartitionFilters [{scan_filters(q)[:100]}]")
    q.collect()
    m = scan_metrics()
    print(f"    partitions read {m.get('number of partitions read', '?')}, rows out of scan {m.get('number of output rows', '?')}")
spark.conf.unset("spark.sql.cbo.enabled")
