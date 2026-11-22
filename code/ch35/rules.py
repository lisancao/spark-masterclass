"""Chapter 35: what Spark 4.3 checks in a metric view definition, and in a query of one.

Run from code/ after ch35/define.py:  uv run python ch35/rules.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mv import attempt, mv_session

spark = mv_session("ch35-rules")
spark.range(3).selectExpr("id AS pickup_zone_id").createOrReplaceTempView("zones_temp")

BASE = """
version: 0.1
source: taxi.trips
dimensions:
  - name: borough
    expr: pickup_borough
measures:
  - name: trips
    expr: COUNT(1)
"""


def create(definition, name="taxi.rules_check", extra=""):
    spark.sql(f"DROP VIEW IF EXISTS {name}")
    return f"CREATE VIEW {name} {extra} WITH METRICS LANGUAGE YAML AS $${definition}$$"


print("== definitions")
attempt(spark, "the base definition", create(BASE))
attempt(spark, "version: 1.0", create(BASE.replace("version: 0.1", "version: 1.0")))
attempt(spark, "no measures and no dimensions", create("version: 0.1\nsource: taxi.trips\n"))
attempt(spark, "  ... and a query of it", "SELECT count(*) FROM taxi.rules_check", show=True)
attempt(spark, "a joins key", create(BASE + "joins:\n  - name: zones\n    source: taxi.zones\n"))
attempt(spark, "a query as the source",
        create(BASE.replace("source: taxi.trips", "source: SELECT * FROM taxi.trips WHERE payment_type = 1")))
attempt(spark, "a temporary view as the source", create(BASE.replace("source: taxi.trips", "source: zones_temp")))
attempt(spark, "a measure with no aggregate function", create(BASE.replace("COUNT(1)", "fare_amount")))
attempt(spark, "a dimension with an aggregate function",
        create(BASE.replace("expr: pickup_borough", "expr: max(pickup_borough)")))
attempt(spark, "a column that the source does not have", create(BASE.replace("COUNT(1)", "SUM(tolls)")))
attempt(spark, "two columns with one name", create(BASE.replace("name: trips", "name: borough")))
attempt(spark, "CREATE TEMPORARY VIEW ... WITH METRICS",
        f"CREATE TEMPORARY VIEW rules_temp WITH METRICS LANGUAGE YAML AS $${BASE}$$")
attempt(spark, "a column list on the view", create(BASE, extra="(b, t)"))

print("\n== queries")
spark.sql(create(BASE))
attempt(spark, "SELECT *", "SELECT * FROM taxi.rules_check")
attempt(spark, "a measure without MEASURE()", "SELECT borough, trips FROM taxi.rules_check GROUP BY borough")
attempt(spark, "a dimension and a measure without GROUP BY",
        "SELECT borough, MEASURE(trips) FROM taxi.rules_check")
attempt(spark, "a source column that is not a dimension",
        "SELECT payment_type, MEASURE(trips) FROM taxi.rules_check GROUP BY payment_type")
attempt(spark, "MEASURE() of a dimension", "SELECT MEASURE(borough) FROM taxi.rules_check")
attempt(spark, "MEASURE() in ORDER BY",
        "SELECT borough, MEASURE(trips) AS t FROM taxi.rules_check GROUP BY borough "
        "ORDER BY MEASURE(trips) DESC LIMIT 1", show=True)
attempt(spark, "MEASURE() in HAVING",
        "SELECT borough, MEASURE(trips) AS t FROM taxi.rules_check GROUP BY borough "
        "HAVING MEASURE(trips) > 1000000", show=True)
attempt(spark, "MEASURE() in a window",
        "SELECT borough, MEASURE(trips) AS t, sum(MEASURE(trips)) OVER () AS total FROM taxi.rules_check "
        "GROUP BY borough ORDER BY t DESC LIMIT 2", show=True)
attempt(spark, "the alias of a measure in ORDER BY",
        "SELECT borough, MEASURE(trips) AS t FROM taxi.rules_check GROUP BY borough ORDER BY t DESC LIMIT 1",
        show=True)
attempt(spark, "a join of the metric view to a table",
        "SELECT m.borough, MEASURE(m.trips) FROM taxi.rules_check m JOIN taxi.trips t "
        "ON m.borough = t.pickup_borough GROUP BY m.borough")
spark.sql("DROP VIEW IF EXISTS taxi.rules_check")
