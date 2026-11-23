"""Chapter 23: one definition, read at many grains with MEASURE(), and the plan that Spark makes.

Run from code/ after ch23/define.py:  uv run python ch23/grains.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import plan
from mv import mv_session

spark = mv_session("ch23-grains")

print("== the month")
spark.sql("""
    SELECT MEASURE(trips) AS trips, round(MEASURE(fare_per_mile), 4) AS fare_per_mile,
           MEASURE(active_zones) AS active_zones
    FROM taxi.trip_metrics""").show()

print("== by borough")
spark.sql("""
    SELECT borough, MEASURE(trips) AS trips, round(MEASURE(fare_per_mile), 4) AS fare_per_mile,
           MEASURE(active_zones) AS active_zones
    FROM taxi.trip_metrics GROUP BY borough ORDER BY trips DESC""").show()

print("== by day type and payment, with a filter on a dimension")
spark.sql("""
    SELECT day_type, payment, MEASURE(trips) AS trips, round(MEASURE(tip_rate), 4) AS tip_rate
    FROM taxi.trip_metrics WHERE payment IN ('card', 'cash')
    GROUP BY day_type, payment ORDER BY day_type, payment""").show()

print("== the busiest pickup hours in Manhattan, ordered by a measure")
spark.sql("""
    SELECT pickup_hour, MEASURE(trips) AS trips, round(MEASURE(revenue)) AS revenue
    FROM taxi.trip_metrics WHERE borough = 'Manhattan'
    GROUP BY pickup_hour ORDER BY trips DESC LIMIT 3""").show()

print("== the same question from the DataFrame API")
(spark.table("taxi.trip_metrics").where("borough = 'Queens'").groupBy("day_type")
 .agg(F.expr("measure(trips)").alias("trips"),
      F.round(F.expr("measure(fare_per_mile)"), 4).alias("fare_per_mile"))
 .orderBy("day_type").show())

print("== what a metric view does not prevent: aggregate MEASURE() results again")
spark.sql("""
    SELECT round(avg(fpm), 4) AS average_of_borough_measures FROM (
      SELECT borough, MEASURE(fare_per_mile) AS fpm FROM taxi.trip_metrics GROUP BY borough)""").show()
spark.sql("""
    SELECT sum(z) AS sum_of_daily_measures FROM (
      SELECT pickup_date, MEASURE(active_zones) AS z FROM taxi.trip_metrics GROUP BY pickup_date)""").show()

print("== how MEASURE() becomes a plan")
q = spark.sql("""SELECT borough, MEASURE(fare_per_mile) AS fare_per_mile
                 FROM taxi.trip_metrics WHERE day_type = 'weekend' GROUP BY borough""")
for mode, marker in (("extended", "== Analyzed Logical Plan =="), ("extended", "== Optimized Logical Plan ==")):
    text = plan(q, mode)
    block = text.split(marker)[1].split("==")[0].strip().splitlines()
    print(marker)
    for line in block:
        print("  " + line[:150])
print("== Physical Plan ==")
for line in plan(q, "formatted").split("\n\n")[0].splitlines():
    print("  " + line)
