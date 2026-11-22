"""Chapter 61, Step 9: a StreamingQueryListener in Python, with observe() to see schema drift.

The three event files are SparkTutor's sample data; the second file renames `user` to `user_id`.
Runs in Classic, then (with --connect) against the Part VIII Connect server, and prints the PID that
ran each callback.

Run from code/:  uv run python ch61/stream_listener.py   and   ... stream_listener.py --connect
"""
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.streaming import StreamingQueryListener  # noqa: E402

EVENTS = str(Path(__file__).resolve().parent / "data" / "events")
CONNECT = "--connect" in sys.argv
if CONNECT:
    os.environ["SPARK_REMOTE"] = f"sc://localhost:{os.environ.get('CONNECT_PORT', '10280')}"


class Collector(StreamingQueryListener):
    def __init__(self):
        self.events = []

    def onQueryStarted(self, e):
        self.events.append(("started", e.name, os.getpid()))

    def onQueryProgress(self, e):
        p = e.progress
        self.events.append(("progress", p.batchId, p.numInputRows, p.observedMetrics.get("quality"),
                            p.durationMs, os.getpid()))

    def onQueryIdle(self, e):
        self.events.append(("idle", os.getpid()))

    def onQueryTerminated(self, e):
        self.events.append(("terminated", e.exception, os.getpid()))


spark = session("stream-listener")
c = Collector()
spark.streams.addListener(c)
ck = tempfile.mkdtemp(prefix="ch61-ck-")
schema = "event STRING, user STRING, user_id STRING, amount DOUBLE, ts STRING"
q = (spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(EVENTS)
     .observe("quality", F.count(F.lit(1)).alias("rows"),
              F.sum(F.col("user").isNull().cast("int")).alias("null_user"),
              F.sum(F.col("event").isNull().cast("int")).alias("null_event"))
     .writeStream.format("noop").option("checkpointLocation", ck)
     .trigger(availableNow=True).queryName("events_q").start())
q.awaitTermination()
for _ in range(50):                               # callbacks arrive asynchronously
    if c.events and c.events[-1][0] == "terminated":
        break
    time.sleep(0.2)

print(f"{'connect' if CONNECT else 'classic'}: this process is PID {os.getpid()}")
for e in c.events:
    if e[0] == "progress":
        _, batch, n, quality, d, pid = e
        print(f"  progress batch {batch}: {n} rows, quality={dict(quality.asDict()) if quality else None}, pid {pid}")
        print(f"      durationMs: {dict(sorted(d.items()))}")
    else:
        print("  ", e)

last = q.lastProgress
try:
    import json
    json.dumps(last)
except TypeError as err:
    print("json.dumps(q.lastProgress):", type(err).__name__, err)
print("q.lastProgress.json starts:", last.json[:60])
spark.streams.removeListener(c)                   # before stop: Connect needs an active session
shutil.rmtree(ck, ignore_errors=True)
spark.stop()
