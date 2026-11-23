"""Step 9: the older API, applyInPandasWithState, with the same session rule as the Sessionizer,
and a move of its open sessions into transformWithState through the state data source.

Run from code/:  uv run python ch41/t6_legacy.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from common.book import session, work, error_text, ROCKSDB
from checkout import drop, SCHEMA
from t2_sessions_lib import Sessionizer, GAP_MS

spark = session("ch41-legacy", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC",
                spark_ui_showConsoleProgress="false")
w = work("ch41/t6")
inbox = w / "inbox"
inbox.mkdir()
OUT = "user STRING, start_ms LONG, end_ms LONG, clicks INT, revenue DOUBLE, closed_by STRING"
STATE = "start_ms LONG, last_ms LONG, clicks INT, revenue DOUBLE, seen STRING"   # seen: event ids, comma-separated
hm = lambda ms: F.date_format(F.timestamp_millis(ms), "HH:mm:ss")


def sessions(key, pdfs, state):
    """One state tuple and one timeout per user. Spark calls this for new rows OR for a timeout."""
    user = key[0]
    if state.hasTimedOut:                                     # called for the timeout, with no rows
        s = state.get
        state.remove()
        yield pd.DataFrame([dict(user=user, start_ms=s[0], end_ms=s[1], clicks=s[2], revenue=round(s[3], 2), closed_by="timeout")])
        return
    s = state.get if state.exists else None
    closed = []
    rows = pd.concat(list(pdfs)).sort_values("ts")
    for r in rows.itertuples():
        if s is not None and r.event_id in s[4].split(","):
            continue                                          # a re-sent event
        ms = int(r.ts.timestamp() * 1000)
        if s is not None and ms > s[1] + GAP_MS:
            closed.append(dict(user=user, start_ms=s[0], end_ms=s[1], clicks=s[2], revenue=round(s[3], 2), closed_by="gap"))
            s = None
        amount = 0.0 if pd.isna(r.amount) else r.amount
        s = (ms, ms, 1, amount, r.event_id) if s is None else \
            (min(s[0], ms), max(s[1], ms), s[2] + 1, s[3] + amount, s[4] + "," + r.event_id)
    state.update(s)
    state.setTimeoutTimestamp(s[1] + GAP_MS)                  # replaces the old timeout of this user
    yield pd.DataFrame(closed, columns=OUT_COLS)


OUT_COLS = ["user", "start_ms", "end_ms", "clicks", "revenue", "closed_by"]
events = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(inbox))
          .withWatermark("ts", "10 minutes"))
legacy = events.groupBy("user").applyInPandasWithState(
    sessions, outputStructType=OUT, stateStructType=STATE,
    outputMode="append", timeoutConf=GroupStateTimeout.EventTimeTimeout)

print("1. applyInPandasWithState on drops 1 to 3, default state store (HDFS-backed):")
for i in range(1, 4):
    drop(inbox, i)
ck = w / "ck_legacy"
q = (legacy.writeStream.format("parquet").option("path", str(w / "out_legacy")).outputMode("append")
     .option("checkpointLocation", str(ck)).trigger(availableNow=True).start())
q.awaitTermination()
print("   provider:", spark.conf.get("spark.sql.streaming.stateStore.providerClass").rsplit(".", 1)[-1])
print("   batches (id, input rows):", [(p.batchId, p.numInputRows) for p in q.recentProgress])
spark.read.parquet(str(w / "out_legacy")).select(
    "user", hm("start_ms").alias("start"), hm("end_ms").alias("end"), "clicks", "revenue", "closed_by"
).orderBy("user", "start").show()

print("2. What the checkpoint keeps (state-metadata, then the state data source):")
spark.read.format("state-metadata").load(str(ck)).select("operatorName", "stateStoreName", "numPartitions").show(truncate=False)
old = spark.read.format("statestore").load(str(ck))
old.printSchema()
old.select("key.user", hm("value.groupState.start_ms").alias("start"), hm("value.groupState.last_ms").alias("last"),
           "value.groupState.clicks", "value.groupState.seen", hm("value.timeoutTimestamp").alias("timeout_at")
           ).orderBy("user").show(truncate=False)


class MigratedSessionizer(Sessionizer):
    """The Sessionizer of Step 4, plus handleInitialState: load one open session from the old state."""

    def handleInitialState(self, key, init, timerValues):
        self.session.update((init.start_ms, init.last_ms, init.clicks, init.revenue))
        for e in init.seen.split(","):
            self.seen.updateValue((e,), (init.last_ms,))
        self.handle.registerTimer(init.last_ms + GAP_MS)


print("3. Move the open sessions into transformWithState (new checkpoint, RocksDB), then read drop 4:")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", ROCKSDB)
initial = old.select("key.user", "value.groupState.*").groupBy("user")
inbox4 = w / "inbox4"
inbox4.mkdir()
drop(inbox4, 4)                                          # the new query's source starts after drop 3
tws = (spark.readStream.schema(SCHEMA).json(str(inbox4)).withWatermark("ts", "10 minutes")
       .groupBy("user").transformWithState(MigratedSessionizer(), OUT, "Append", "EventTime", initialState=initial))
q = (tws.writeStream.format("parquet").option("path", str(w / "out_tws")).outputMode("append")
     .option("checkpointLocation", str(w / "ck_tws")).trigger(availableNow=True).start())
q.awaitTermination()
print("   batches (id, input rows):", [(p.batchId, p.numInputRows) for p in q.recentProgress])
spark.read.parquet(str(w / "out_tws")).select(
    "user", hm("start_ms").alias("start"), hm("end_ms").alias("end"), "clicks", "revenue", "closed_by"
).orderBy("user", "start").show()
print("   still open after drop 4:")
(spark.read.format("statestore").option("stateVarName", "session").load(str(w / "ck_tws"))
 .select("key.user", hm("value.start_ms").alias("start"), hm("value.last_ms").alias("last"), "value.clicks")
 .orderBy("user").show())

print("4. Restart the old checkpoint with transformWithState, and give it drop 4:")
drop(inbox, 4)
try:
    q = (events.groupBy("user").transformWithState(Sessionizer(), OUT, "Append", "EventTime")
         .writeStream.format("parquet").option("path", str(w / "out_x")).outputMode("append")
         .option("checkpointLocation", str(ck)).trigger(availableNow=True).start())
    q.awaitTermination()
    print("   it ran:", [(p.batchId, p.numInputRows) for p in q.recentProgress])
except Exception as e:
    print(error_text(e, 86, 3))
spark.stop()
