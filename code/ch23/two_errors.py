"""Chapter 23: two aggregation errors that run without an error.

Run from code/ after ch23/prepare.py:  uv run python ch23/two_errors.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mv import mv_session

spark = mv_session("ch23-two-errors")

print("== 1. fare per mile: the average of borough ratios, and the ratio of the totals")
by_borough = spark.sql("""
    SELECT pickup_borough, count(*) AS trips,
           round(sum(fare_amount) / sum(trip_distance), 4) AS fare_per_mile
    FROM taxi.trips GROUP BY pickup_borough ORDER BY trips DESC""")
by_borough.show()
by_borough.createOrReplaceTempView("by_borough")
spark.sql("""
    SELECT (SELECT round(avg(fare_per_mile), 4) FROM by_borough) AS average_of_borough_ratios,
           (SELECT round(sum(fare_amount) / sum(trip_distance), 4) FROM taxi.trips) AS ratio_of_totals
""").show()

print("== 2. active pickup zones: the sum of daily distinct counts, and the distinct count")
spark.sql("""
    SELECT (SELECT sum(zones) FROM (SELECT pickup_date, count(DISTINCT pickup_zone_id) AS zones
                                    FROM taxi.trips GROUP BY pickup_date)) AS sum_of_daily_counts,
           (SELECT round(avg(zones), 1) FROM (SELECT pickup_date, count(DISTINCT pickup_zone_id) AS zones
                                    FROM taxi.trips GROUP BY pickup_date)) AS average_day,
           (SELECT count(DISTINCT pickup_zone_id) FROM taxi.trips) AS distinct_in_june
""").show()
