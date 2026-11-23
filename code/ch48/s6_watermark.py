"""Step 6: a watermark advancing over real late data.

Trips arrive when they end (one file per 5 minutes of dropoffs) and are counted by
pickup time in 10-minute windows, in append mode. The watermark is the newest pickup
seen so far minus the delay. A window is written once the watermark passes its end;
a trip whose window has already closed is dropped.
Run from code/:  uv run python ch48/s6_watermark.py [delay, e.g. "15 minutes"] [--quiet]
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver
from replay import slices, SCHEMA

DELAY = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else "15 minutes"
QUIET = "--quiet" in sys.argv
spark = session("ch48-watermark", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC")
w = work(f"ch48/s6-{DELAY.split()[0]}")
inbox = w / "inbox"
inbox.mkdir()
files = slices()
for k, (name, text, n) in enumerate(files):
    deliver(inbox, name, text, mtime=1_000_000 + k)        # file order = arrival order

trips = (spark.readStream.schema(SCHEMA)
         .option("maxFilesPerTrigger", 1)                  # one 5-minute file per micro-batch
         .json(str(inbox)))
counts = (trips
          .withWatermark("pickup_ts", DELAY)
          .groupBy(F.window("pickup_ts", "10 minutes").alias("w"))
          .agg(F.count("*").alias("trips")))
q = (counts.writeStream.format("memory").queryName("windows")
     .outputMode("append")
     .option("checkpointLocation", str(w / "checkpoint"))
     .trigger(availableNow=True)
     .start())
q.awaitTermination()

hm = lambda s: s[11:16] if s else "  -  "
if not QUIET:
    print(f"watermark delay {DELAY}; one file of dropoffs per batch\n")
    print("batch  file   rows  max pickup  watermark  dropped  windows out  state rows")
dropped = 0
for p in q.recentProgress:
    op = p.stateOperators[0]
    dropped += op.numRowsDroppedByWatermark
    if QUIET:
        continue
    f = files[p.batchId][0][6:10] if p.numInputRows else "(none)"
    print(f"{p.batchId:>5}  {f:<6} {p.numInputRows:>4}  {hm(p.eventTime.get('max')):^10}  "
          f"{hm(p.eventTime.get('watermark')):^9}  {op.numRowsDroppedByWatermark:>7}  "
          f"{p.sink.numOutputRows:>11}  {op.numRowsTotal:>10}")

emitted = spark.table("windows").select(F.date_format("w.start", "MM-dd HH:mm").alias("window"), "trips")
truth = (spark.read.schema(SCHEMA).json(str(inbox))
         .groupBy(F.date_format(F.window("pickup_ts", "10 minutes").start, "MM-dd HH:mm").alias("window"))
         .agg(F.count("*").alias("all_trips")))
cmp = truth.join(emitted, "window", "left").orderBy("window").collect()
written = [r for r in cmp if r.trips is not None]
lost = sum(r.all_trips - r.trips for r in written)
print(f"\ndelay {DELAY}: {len(written)} windows written, holding {sum(r.trips for r in written)} trips; "
      f"{lost} trips in them arrived too late\n(numRowsDroppedByWatermark summed to {dropped})")
if not QUIET:
    print("window  all trips  written")
    for r in cmp:
        if "06-10 07:30" <= r.window <= "06-10 08:50":
            print(f" {r.window[6:]}  {r.all_trips:>9}  {r.trips if r.trips is not None else '(open)':>7}")
