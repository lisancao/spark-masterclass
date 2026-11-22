"""Chapter 35: one month of NYC taxi trips as a table, with the pickup borough from the zone list.

Run from code/:  uv run python ch35/prepare.py
"""
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mv import WORK, TRIPS, ZONES, mv_session

shutil.rmtree(WORK, ignore_errors=True)
spark = mv_session("ch35-prepare")
spark.sql("CREATE DATABASE taxi")
spark.read.parquet(str(TRIPS)).createOrReplaceTempView("raw_trips")
spark.read.option("header", True).csv(str(ZONES)).createOrReplaceTempView("zones")
spark.sql("""
    CREATE TABLE taxi.trips USING parquet AS
    SELECT t.tpep_pickup_datetime AS pickup_ts,
           CAST(t.tpep_pickup_datetime AS DATE) AS pickup_date,
           t.PULocationID AS pickup_zone_id,
           z.Borough AS pickup_borough,
           t.payment_type, t.trip_distance, t.fare_amount, t.tip_amount, t.total_amount
    FROM raw_trips t JOIN zones z ON t.PULocationID = CAST(z.LocationID AS INT)
    WHERE t.tpep_pickup_datetime >= '2026-06-01' AND t.tpep_pickup_datetime < '2026-07-01'
      AND t.trip_distance > 0 AND t.trip_distance < 100 AND t.fare_amount > 0
""")
print("taxi.trips:", spark.table("taxi.trips").count(), "rows")
spark.sql("""SELECT pickup_borough, count(*) AS trips FROM taxi.trips
             GROUP BY 1 ORDER BY trips DESC""").show()
