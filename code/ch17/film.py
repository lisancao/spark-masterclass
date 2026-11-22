"""Chapter 17, film: the two answers for fare per mile, then MEASURE() by borough.

Run from code/ after ch17/define.py:  uv run python ch17/film.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mv import mv_session

spark = mv_session("ch17-film")
print("fare per mile, by hand:")
spark.sql("""SELECT round(avg(r), 4) AS average_of_borough_ratios FROM (
               SELECT sum(fare_amount) / sum(trip_distance) AS r FROM taxi.trips
               WHERE pickup_borough NOT IN ('Unknown', 'N/A') GROUP BY pickup_borough)""").show()
print("fare per mile, from the metric view:")
spark.sql("SELECT round(MEASURE(fare_per_mile), 4) AS fare_per_mile FROM taxi.trip_metrics").show()
spark.sql("""SELECT borough, MEASURE(trips) AS trips, round(MEASURE(fare_per_mile), 4) AS fare_per_mile
             FROM taxi.trip_metrics GROUP BY borough ORDER BY trips DESC LIMIT 4""").show()
