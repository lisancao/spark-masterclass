"""Step 13: the StreamingQueryManager (spark.streams). Many queries in one session: list them,
find one by id, wait for any of them to stop, and listen to all of them.

Run from code/:  uv run python ch48/s13_manager.py
"""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from common.book import session, work, deliver, error_text
from replay import slices, SCHEMA

spark = session("ch48-manager", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC",
                spark_ui_showConsoleProgress="false")
w = work("ch48/s13")
(w / "inbox").mkdir()
files = slices()
t0 = time.time()
clock = lambda: f"{time.time() - t0:5.1f}s"


class Events(StreamingQueryListener):
    """One listener for every query of the session."""
    def onQueryStarted(self, e):
        print(f"   {clock()}  listener: started    {e.name}", flush=True)

    def onQueryProgress(self, e):
        if e.progress.numInputRows:
            print(f"   {clock()}  listener: progress   {e.progress.name} batch {e.progress.batchId}"
                  f" rows {e.progress.numInputRows}", flush=True)

    def onQueryIdle(self, e):
        pass

    def onQueryTerminated(self, e):
        name = names.get(str(e.id), "?")
        print(f"   {clock()}  listener: terminated {name}  exception: {(e.exception or 'None')[:60]}", flush=True)


names = {}
listener = Events()
spark.streams.addListener(listener)
trips = spark.readStream.schema(SCHEMA).json(str(w / "inbox"))


@F.udf("double")
def strict_fare(fare):
    if fare is not None and fare < 0:
        raise ValueError(f"negative fare {fare}")
    return fare


def start(name, df, mode="append"):
    q = (df.writeStream.queryName(name).format("memory").outputMode(mode)
         .option("checkpointLocation", str(w / name)).trigger(processingTime="1 second").start())
    names[q.id] = name
    return q


print("1. Three queries on one stream of trips:")
q1 = start("by_zone", trips.groupBy("PULocationID").count(), "complete")
q2 = start("long_trips", trips.where("trip_distance > 20"))
q3 = start("strict", trips.select("trip_id", strict_fare("fare_amount").alias("fare")))
time.sleep(4)
print("\n2. spark.streams.active:")
for q in spark.streams.active:
    print(f"   {q.name:<11} id={q.id[:8]}  active={q.isActive}  status={q.status['message']}")
print("   spark.streams.get(id) ->", spark.streams.get(q2.id).name)

print("\n3. awaitAnyTermination(timeout=3) while all run:", spark.streams.awaitAnyTermination(3))

print("\n4. Deliver files. The second file has two trips with a negative fare:")
deliver(w / "inbox", files[0][0], files[0][1])
time.sleep(3)
deliver(w / "inbox", files[1][0], files[1][1])      # this morning has refunds: fare_amount < 0
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print(f"   {clock()}  awaitAnyTermination raised:")
    print("  " + error_text(e, 84, 1))
    print("     ...", next(l.strip() for l in str(e).splitlines() if l.strip().startswith("ValueError")))

print("\n5. After the failure:")
print("   active:", sorted(q.name for q in spark.streams.active))
print("   strict.isActive:", q3.isActive, "  strict.exception() is set:", q3.exception() is not None)
print("   awaitAnyTermination(1) again ->", end=" ")
try:
    print(spark.streams.awaitAnyTermination(1))
except Exception as e:
    print("raises again:", type(e).__name__)
spark.streams.resetTerminated()
print("   after resetTerminated(), awaitAnyTermination(1) ->", spark.streams.awaitAnyTermination(1))

print("\n6. Stop the others, then remove the listener:")
for q in spark.streams.active:
    q.stop()
time.sleep(1)
spark.streams.removeListener(listener)
print("   active:", spark.streams.active)
spark.stop()
