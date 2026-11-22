"""Step 6: processing time. TTL on state, processing-time timers, the time-mode rules,
and what a TIMESTAMP looks like after a round trip through state.

Run from code/:  uv run python ch26/t3_time.py
"""
import datetime as dt
import sys
import threading
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessor
from common.book import session, work, deliver, error_text, ROCKSDB

spark = session("ch26-time", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC",
                spark_sql_streaming_stateStore_providerClass=ROCKSDB)
T0 = time.time()
clock = lambda: f"{time.time() - T0:5.1f}s"


class LastSeen(StatefulProcessor):
    """Remembers each user's last page for 8 s (TTL), and nudges a user 5 s after the last click (timer)."""

    def init(self, handle):
        self.handle = handle
        self.last = handle.getValueState("last", "page STRING", ttlDurationMs=8000)

    def handleInputRows(self, key, rows, timerValues):
        had = self.last.get()[0] if self.last.exists() else None
        for r in rows:
            self.last.update((r.page,))
        for t in self.handle.listTimers():
            self.handle.deleteTimer(t)
        self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 5000)
        yield Row(user=key[0], event="click", remembered_before=had)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        had = self.last.get()[0] if self.last.exists() else None
        yield Row(user=key[0], event="timer", remembered_before=had)

    def close(self):
        pass


w = work("ch26/t3")
inbox = w / "inbox"
inbox.mkdir()
out = (spark.readStream.schema("user STRING, page STRING").json(str(inbox))
       .groupBy("user").transformWithState(LastSeen(), "user STRING, event STRING, remembered_before STRING",
                                           "Update", "ProcessingTime"))


def show(df, batch_id):
    for r in df.where("user != 'w'").collect():
        print(f"{clock()}  batch {batch_id:>2}  {r.user}  {r.event:<5}  state before: {r.remembered_before}")


q = (out.writeStream.foreachBatch(show).outputMode("update")
     .option("checkpointLocation", str(w / "ck")).trigger(processingTime="1 second").start())
deliver(inbox, "warmup.json", '{"user": "w", "page": "/"}\n')     # the first batch starts the Python workers
while not q.recentProgress or q.recentProgress[-1].numInputRows == 0:
    time.sleep(0.2)
time.sleep(6)
print(f"{clock()}  (warm-up done)")
plan = [(0, "a", "/p/desk"), (3, "a", "/cart"), (4, "b", "/"), (15, "a", "/p/lamp")]
start = time.time()
for at, user, page in plan:
    time.sleep(max(0, at - (time.time() - start)))
    deliver(inbox, f"{at:02d}.json", f'{{"user": "{user}", "page": "{page}"}}\n')
    print(f"{clock()}  + click {user} {page}")
time.sleep(7)
q.stop()

print("\nThe time-mode rules:")
for mode in ("None", "EventTime"):
    try:
        q = (spark.readStream.schema("user STRING, page STRING").json(str(inbox))
             .groupBy("user").transformWithState(LastSeen(), "user STRING, event STRING, remembered_before STRING",
                                                 "Update", mode)
             .writeStream.format("noop").outputMode("update")
             .option("checkpointLocation", str(w / f"ck_{mode}")).trigger(availableNow=True).start())
        q.awaitTermination(60)
        print(f"  timeMode={mode}: ran")
    except Exception as e:
        import re
        m = re.search(r"\[(STATEFUL_PROCESSOR[A-Z_.]*)\]([^\n]*)", str(e))
        print(f"  timeMode={mode}: [{m.group(1)}]\n   {m.group(2).strip()[:170]}" if m else error_text(e))


class Echo(StatefulProcessor):
    def init(self, handle):
        self.ts = handle.getValueState("ts", "ts TIMESTAMP")

    def handleInputRows(self, key, rows, timerValues):
        for r in rows:
            self.ts.update((r.ts,))
            yield Row(user=key[0], input_ts=str(r.ts), from_state=str(self.ts.get()[0]))

    def close(self):
        pass


print("\nA TIMESTAMP through state (session time zone UTC, the worker's zone is the machine's):")
deliver(inbox, "ts.json", '{"user": "z", "page": "/", "ts": "2026-10-05T09:00:00Z"}\n')
q = (spark.readStream.schema("user STRING, page STRING, ts TIMESTAMP").json(str(inbox)).where("ts IS NOT NULL")
     .groupBy("user").transformWithState(Echo(), "user STRING, input_ts STRING, from_state STRING", "Update", "None")
     .writeStream.format("memory").queryName("echo").outputMode("update")
     .option("checkpointLocation", str(w / "ck_echo")).trigger(availableNow=True).start())
q.awaitTermination()
for r in spark.table("echo").collect():
    print(f"  in handleInputRows: {r.input_ts}\n  read back from state: {r.from_state}")
print("  machine time zone:", time.strftime("%Z"))
