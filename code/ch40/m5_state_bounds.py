"""Step 6: state that stays bounded. dropDuplicates against dropDuplicatesWithinWatermark,
and the global watermark when two inputs run at different speeds.

Run from code/:  uv run python ch40/m5_state_bounds.py
"""
import datetime as dt
import json
import random
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver

spark = session("ch40-state-bounds", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC")
rng = random.Random(7)
T0 = dt.datetime(2026, 10, 5, 9, 0)
SCHEMA = "event_id STRING, user_id STRING, ts TIMESTAMP"


def batch_lines(b, n=1000, resend=0.02, lag_min=0):
    """n events for minutes [10b, 10b+10), about 2% sent twice; lag_min shifts the clock back."""
    out = []
    for k in range(n):
        ts = T0 + dt.timedelta(minutes=10 * b - lag_min, seconds=rng.uniform(0, 600))
        e = {"event_id": f"b{b}-{k}", "user_id": f"u{rng.randrange(50)}", "ts": ts.isoformat()}
        out.append(json.dumps(e))
        if rng.random() < resend:
            out.append(json.dumps(e))
    return "\n".join(out) + "\n"


print("1. Six batches of 1,000 events, 10 minutes of event time each, ~2% re-sent")
w = work("ch40/m5-dedup")
(w / "inbox").mkdir()
for b in range(6):
    deliver(w / "inbox", f"b{b}.jsonl", batch_lines(b), mtime=1_000_000 + b)
src = lambda: spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "inbox"))
variants = {
    "dropDuplicates": src().dropDuplicates(["event_id"]),
    "dropDuplicatesWithinWatermark": src().withWatermark("ts", "10 minutes")
                                          .dropDuplicatesWithinWatermark(["event_id"]),
}
for name, df in variants.items():
    q = (df.writeStream.format("noop").option("checkpointLocation", str(w / f"ck_{name}"))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    rows = [p.stateOperators[0].numRowsTotal for p in q.recentProgress if p.numInputRows]
    out = sum(p.stateOperators[0].numRowsUpdated for p in q.recentProgress)
    print(f"   {name:<30} state rows after each batch {rows}  (unique events kept {out})")

print("\n2. Two inputs, one 30 minutes behind: which watermark wins?")
for policy in ("min", "max"):
    spark.conf.set("spark.sql.streaming.multipleWatermarkPolicy", policy)
    w = work(f"ch40/m5-policy-{policy}")
    for side, lag in (("web", 0), ("phone", 30)):
        (w / side).mkdir()
        for b in range(6):
            deliver(w / side, f"b{b}.jsonl", batch_lines(b, n=200, resend=0, lag_min=lag), mtime=1_000_000 + b)
    web = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "web")) \
        .withWatermark("ts", "5 minutes")
    phone = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "phone")) \
        .withWatermark("ts", "5 minutes")
    counts = web.union(phone).groupBy(F.window("ts", "10 minutes")).count()
    q = (counts.writeStream.format("memory").queryName(f"policy_{policy}").outputMode("update")
         .option("checkpointLocation", str(w / "ck")).trigger(availableNow=True).start())
    q.awaitTermination()
    counted = spark.table(f"policy_{policy}").groupBy("window").agg(F.max("count").alias("n")) \
        .agg(F.sum("n")).first()[0]
    wms = [p.eventTime.get("watermark", "")[11:16] for p in q.recentProgress]
    late = sum(p.stateOperators[0].numRowsDroppedByWatermark for p in q.recentProgress)
    print(f"   policy {policy}: global watermark per batch {wms}")
    print(f"     events counted {counted} of 2400; numRowsDroppedByWatermark {late}")
