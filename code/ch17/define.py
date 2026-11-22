"""Chapter 17: define a metric view over taxi.trips, and see what Spark stores for it.

Run from code/ after ch17/prepare.py:  uv run python ch17/define.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mv import mv_session

spark = mv_session("ch17-define")

TRIP_METRICS = """
version: 0.1
source: taxi.trips
filter: pickup_borough NOT IN ('Unknown', 'N/A')
dimensions:
  - name: borough
    expr: pickup_borough
  - name: pickup_date
    expr: pickup_date
  - name: pickup_hour
    expr: hour(pickup_ts)
  - name: day_type
    expr: CASE WHEN dayofweek(pickup_date) IN (1, 7) THEN 'weekend' ELSE 'weekday' END
  - name: payment
    expr: CASE payment_type WHEN 1 THEN 'card' WHEN 2 THEN 'cash' ELSE 'other' END
measures:
  - name: trips
    expr: COUNT(1)
  - name: revenue
    expr: SUM(total_amount)
  - name: fare_per_mile
    expr: SUM(fare_amount) / SUM(trip_distance)
  - name: tip_rate
    expr: SUM(tip_amount) / SUM(fare_amount)
  - name: active_zones
    expr: COUNT(DISTINCT pickup_zone_id)
"""
spark.sql("DROP VIEW IF EXISTS taxi.trip_metrics")
spark.sql(f"CREATE VIEW taxi.trip_metrics WITH METRICS LANGUAGE YAML AS $${TRIP_METRICS}$$")
print("created taxi.trip_metrics\n")

print("== DESCRIBE TABLE EXTENDED taxi.trip_metrics (selected rows)")
for r in spark.sql("DESCRIBE TABLE EXTENDED taxi.trip_metrics").collect():
    name, value = r.col_name.strip(), (r.data_type or "").strip()
    if name in ("borough", "pickup_hour", "trips", "fare_per_mile", "active_zones"):
        print(f"  {name:<20} {value}")
    elif name in ("Type", "Table Properties"):
        print(f"  {name:<20} {value}")
    elif name == "View Text":
        print(f"  {name:<20} {value.splitlines()[0]} ... ({len(value.splitlines())} lines)")

print("\n== the column metadata that marks dimensions and measures")
for f in spark.table("taxi.trip_metrics").schema.fields:
    print(f"  {f.name:<14} {f.dataType.simpleString():<8} {f.metadata}")
