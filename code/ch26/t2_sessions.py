"""Steps 4-5: sessions with event-time timers. One ValueState and one MapState per user;
a session ends on a gap of more than 30 minutes (seen in handleInputRows) or when its
timer fires (handleExpiredTimer). Then the open sessions and timers, read from state.

Run from code/:  uv run python ch26/t2_sessions.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Row, functions as F
from pyspark.sql.streaming import StatefulProcessor
from common.book import session, work, ROCKSDB
from checkout import drop, SCHEMA

GAP_MS = 30 * 60 * 1000


def to_ms(ts):
    return int(ts.timestamp() * 1000)      # input timestamps arrive as aware datetimes; keep epoch ms


class Sessionizer(StatefulProcessor):
    def init(self, handle):
        self.handle = handle
        self.session = handle.getValueState("session", "start_ms LONG, last_ms LONG, clicks INT, revenue DOUBLE")
        self.seen = handle.getMapState("seen", "event_id STRING", "ts_ms LONG")

    def _closed(self, user, s, how):
        return Row(user=user, start_ms=s[0], end_ms=s[1], clicks=s[2], revenue=round(s[3], 2), closed_by=how)

    def handleInputRows(self, key, rows, timerValues):
        user = key[0]
        s = self.session.get() if self.session.exists() else None
        for r in sorted(rows, key=lambda r: r.ts):
            if self.seen.containsKey((r.event_id,)):
                continue                                    # a re-sent event
            ms = to_ms(r.ts)
            if s is not None and ms > s[1] + GAP_MS:
                yield self._closed(user, s, "gap")          # a new click after a long gap
                self.seen.clear()
                s = None
            amount = r.amount or 0.0
            s = (ms, ms, 1, amount) if s is None else (min(s[0], ms), max(s[1], ms), s[2] + 1, s[3] + amount)
            self.seen.updateValue((r.event_id,), (ms,))
        if s is None:
            return
        for t in self.handle.listTimers():                  # one timer per user: move it
            self.handle.deleteTimer(t)
        self.session.update(s)
        self.handle.registerTimer(s[1] + GAP_MS)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        s = self.session.get()
        self.session.clear()
        self.seen.clear()
        yield self._closed(key[0], s, "timer")

    def close(self):
        pass


spark = session("ch26-sessions", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC",
                spark_sql_streaming_stateStore_providerClass=ROCKSDB)
w = work("ch26/t2")
inbox, ck = w / "inbox", w / "checkpoint"
inbox.mkdir()

events = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(inbox))
          .withWatermark("ts", "10 minutes"))
sessions = events.groupBy("user").transformWithState(
    Sessionizer(), "user STRING, start_ms LONG, end_ms LONG, clicks INT, revenue DOUBLE, closed_by STRING",
    "Append", "EventTime")
hm = lambda ms: F.date_format(F.timestamp_millis(ms), "HH:mm:ss")

for i in range(1, 5):            # one drop per micro-batch (maxFilesPerTrigger 1)
    drop(inbox, i)
q = (sessions.writeStream.format("parquet").option("path", str(w / "out")).outputMode("append")
     .option("checkpointLocation", str(ck)).trigger(availableNow=True).start())
q.awaitTermination()
print("batch  rows  watermark  state rows updated")
for p in q.recentProgress:
    print(f"{p.batchId:>5}  {p.numInputRows:>4}  {p.eventTime.get('watermark', '')[11:19]:>9}  "
          f"{p.stateOperators[0].numRowsUpdated:>18}")
out = spark.read.parquet(str(w / "out"))
print("\nClosed sessions:")
out.select("user", hm("start_ms").alias("start"), hm("end_ms").alias("end"), "clicks", "revenue", "closed_by") \
    .orderBy("user", "start").show()

print("Still open, from the state data source: the session value and its timer")
open_ = (spark.read.format("statestore").option("stateVarName", "session").load(str(ck))
         .select("key.user", hm("value.start_ms").alias("start"), hm("value.last_ms").alias("last"), "value.clicks"))
timers = (spark.read.format("statestore").option("readRegisteredTimers", "true").load(str(ck))
          .select("key.user", hm("expiration_timestamp_ms").alias("timer_at")))
open_.join(timers, "user", "left").orderBy("user").show()
print("The map state for u6 (event ids already applied):")
spark.read.format("statestore").option("stateVarName", "seen").load(str(ck)) \
    .where("key.user = 'u6'").select("key.user", "user_map_key.event_id", hm("user_map_value.ts_ms").alias("ts")).show()
