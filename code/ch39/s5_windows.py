"""Step 5: how window(), sliding windows and session_window() assign rows to windows.

The window functions are the same in batch and streaming, so this step runs them on a
batch read of the first three files, where the assignment is easy to see.
Run from code/:  uv run python ch39/s5_windows.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver
from replay import slices, SCHEMA

spark = session("ch39-windows", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC")
inbox = work("ch39/s5") / "inbox"
inbox.mkdir()
for name, text, n in slices()[:3]:
    deliver(inbox, name, text)
trips = spark.read.schema(SCHEMA).json(str(inbox))
one = trips.where("trip_id = '0610-00100'").select("trip_id", "pickup_ts")

print("One trip, three kinds of window:")
one.select("trip_id", "pickup_ts", F.window("pickup_ts", "10 minutes").alias("tumbling")).show(truncate=False)
one.select("trip_id", F.window("pickup_ts", "10 minutes", "5 minutes").alias("sliding")).show(truncate=False)
one.select("trip_id", F.window("pickup_ts", "10 minutes", startTime="3 minutes").alias("offset")).show(truncate=False)

print("Tumbling 10-minute windows over pickups (trips that ended 08:00 to 08:15):")
(trips.groupBy(F.window("pickup_ts", "10 minutes").alias("w")).count()
 .select(F.date_format("w.start", "HH:mm").alias("start"), F.date_format("w.end", "HH:mm").alias("end"), "count")
 .orderBy("start").show(20))

print("Session windows: pickups at JFK (zone 132), a new session after 3 quiet minutes:")
jfk = trips.where("PULocationID = 132")
(jfk.groupBy(F.session_window("pickup_ts", "3 minutes").alias("s")).count()
 .select(F.date_format("s.start", "HH:mm:ss").alias("start"), F.date_format("s.end", "HH:mm:ss").alias("end"), "count")
 .orderBy("start").show(30))
