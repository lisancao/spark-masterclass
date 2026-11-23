"""Chapter 23: replace, inspect, rename and drop a metric view, and where its source resolves.

Run from code/ after ch23/prepare.py:  uv run python ch23/lifecycle.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mv import attempt, mv_session

spark = mv_session("ch23-lifecycle")
for v in ("taxi.lifecycle", "taxi.lifecycle_v2", "taxi.unqualified"):
    spark.sql(f"DROP VIEW IF EXISTS {v}")

V1 = """
version: 0.1
source: taxi.trips
dimensions:
  - name: borough
    expr: pickup_borough
measures:
  - name: trips
    expr: COUNT(1)
"""
V2 = V1 + "  - name: revenue\n    expr: SUM(total_amount)\n"

print("== create, then replace with one more measure")
attempt(spark, "CREATE VIEW taxi.lifecycle", f"CREATE VIEW taxi.lifecycle WITH METRICS LANGUAGE YAML AS $${V1}$$")
attempt(spark, "CREATE VIEW taxi.lifecycle again", f"CREATE VIEW taxi.lifecycle WITH METRICS LANGUAGE YAML AS $${V1}$$")
attempt(spark, "CREATE OR REPLACE VIEW, with revenue",
        f"CREATE OR REPLACE VIEW taxi.lifecycle WITH METRICS LANGUAGE YAML AS $${V2}$$")
attempt(spark, "DROP VIEW, then CREATE VIEW with revenue", "DROP VIEW taxi.lifecycle")
spark.sql(f"CREATE VIEW taxi.lifecycle WITH METRICS LANGUAGE YAML AS $${V2}$$")
attempt(spark, "SELECT MEASURE(revenue)", "SELECT round(MEASURE(revenue)) FROM taxi.lifecycle", show=True)

print("\n== inspect")
attempt(spark, "SHOW CREATE TABLE", "SHOW CREATE TABLE taxi.lifecycle")
attempt(spark, "SHOW VIEWS IN taxi", "SHOW VIEWS IN taxi", show=True)

print("\n== rename, change, drop")
attempt(spark, "ALTER VIEW ... RENAME TO taxi.lifecycle_v2", "ALTER VIEW taxi.lifecycle RENAME TO taxi.lifecycle_v2")
attempt(spark, "SELECT MEASURE(trips) FROM taxi.lifecycle_v2", "SELECT MEASURE(trips) FROM taxi.lifecycle_v2", show=True)
attempt(spark, "ALTER VIEW ... AS SELECT", "ALTER VIEW taxi.lifecycle_v2 AS SELECT pickup_borough FROM taxi.trips")
attempt(spark, "SELECT MEASURE(trips) after ALTER VIEW ... AS", "SELECT MEASURE(trips) FROM taxi.lifecycle_v2")
attempt(spark, "DROP TABLE on a metric view", "DROP TABLE taxi.lifecycle_v2")
attempt(spark, "DROP VIEW", "DROP VIEW taxi.lifecycle_v2")

print("\n== an unqualified source, read from another current database")
spark.sql("USE taxi")
attempt(spark, "CREATE VIEW taxi.unqualified with source: trips",
        f"CREATE VIEW taxi.unqualified WITH METRICS LANGUAGE YAML AS $${V1.replace('source: taxi.trips', 'source: trips')}$$")
attempt(spark, "USE taxi; SELECT MEASURE(trips)", "SELECT MEASURE(trips) FROM taxi.unqualified", show=True)
spark.sql("USE default")
spark.sql("DROP TABLE IF EXISTS default.trips")
spark.sql("CREATE TABLE default.trips USING parquet AS "
          "SELECT 'Nowhere' AS pickup_borough FROM range(5)")
attempt(spark, "USE default; SELECT MEASURE(trips)", "SELECT MEASURE(trips) FROM taxi.unqualified", show=True)
spark.sql("DROP TABLE default.trips")
spark.sql("DROP VIEW taxi.unqualified")
