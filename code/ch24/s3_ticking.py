"""Step 3: a stream that ticks. A feeder drops a file every few seconds, the query runs a
micro-batch on a processing-time trigger, and a StreamingQueryListener prints each batch.

Run from code/:  uv run python ch24/s3_ticking.py [files] [seconds between files]
"""
import sys
import threading
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from common.book import session, work, deliver
from replay import slices, SCHEMA

N = int(sys.argv[1]) if len(sys.argv) > 1 else 6
GAP = float(sys.argv[2]) if len(sys.argv) > 2 else 4.0

spark = session("ch24-ticking", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC")
w = work("ch24/s3")
inbox = w / "inbox"
inbox.mkdir()
T0 = time.time()


def clock():
    return f"{time.time() - T0:5.1f}s"


class Printer(StreamingQueryListener):
    def onQueryStarted(self, e):
        print(f"{clock()}  started   id={str(e.id)[:8]} runId={str(e.runId)[:8]}")

    def onQueryProgress(self, e):
        p = e.progress
        if p.numInputRows == 0:
            return                                    # idle triggers report progress too
        state = p.stateOperators[0].numRowsTotal if p.stateOperators else 0
        print(f"{clock()}  batch {p.batchId:>2}  rows {p.numInputRows:>4}  "
              f"state rows {state:>3}  took {p.durationMs['triggerExecution']:>4} ms")

    def onQueryIdle(self, e):
        pass

    def onQueryTerminated(self, e):
        print(f"{clock()}  stopped")


spark.streams.addListener(Printer())

trips = spark.readStream.schema(SCHEMA).json(str(inbox))
per_zone = trips.groupBy("PULocationID").agg(F.count("*").alias("trips"))
q = (per_zone.writeStream.format("memory").queryName("per_zone")
     .outputMode("update")
     .option("checkpointLocation", str(w / "checkpoint"))
     .trigger(processingTime="2 seconds")
     .start())


def feed():
    for name, text, n in slices()[:N]:
        time.sleep(GAP)
        deliver(inbox, name, text)
        print(f"{clock()}  + {name} ({n} trips)")


feeder = threading.Thread(target=feed)
feeder.start()
feeder.join()
time.sleep(4)
print(f"{clock()}  status: {q.status['message']!r}, active queries: {len(spark.streams.active)}")
q.stop()
time.sleep(0.5)
rows = spark.table("per_zone").count()
print(f"memory sink: {rows} rows appended in update mode "
      f"(one per changed zone per batch), {spark.table('per_zone').select('PULocationID').distinct().count()} zones")
