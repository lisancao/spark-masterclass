"""Step 7: unit tests for a processor with TwsTester (no streaming query), and one probe:
does an availableNow query with timeMode ProcessingTime finish on its own?

Run from code/:  uv run python ch41/t4_tester.py
"""
import datetime as dt
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Row
from pyspark.sql.streaming.tws_tester import TwsTester
from common.book import session, work, deliver, ROCKSDB
from t2_sessions_lib import Sessionizer, GAP_MS

spark = session("ch41-tester", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC",
                spark_sql_streaming_stateStore_providerClass=ROCKSDB)
utc = dt.timezone.utc
at = lambda h, m: dt.datetime(2026, 10, 5, h, m, tzinfo=utc)
ms = lambda d: int(d.timestamp() * 1000)
click = lambda eid, d, amount=None: Row(event_id=eid, user="u1", action="view", page="/", ts=d, amount=amount)

t = TwsTester(Sessionizer(), timeMode="EventTime", outputMode="Append",
              eventTimeExtractor=lambda r: ms(r.ts))
print("1. two clicks, then the same event again:")
print("  ", t.test("u1", [click("c1", at(9, 0)), click("c2", at(9, 10), 5.0)]))
print("  ", t.test("u1", [click("c2", at(9, 10), 5.0)]), "(re-sent: no output)")
print("   session state:", t.peekValueState("session", "u1"))
print("   seen ids:", sorted(k[0] for k in t.peekMapState("seen", "u1")))

print("2. a click 40 minutes later closes the session by gap:")
print("  ", t.test("u1", [click("c3", at(9, 50))]))

print("3. move the watermark past the timer (last click + 30 min):")
print("   setWatermark(09:50 + 29 min) ->", t.setWatermark(ms(at(10, 19))))
print("   setWatermark(09:50 + 30 min) ->", t.setWatermark(ms(at(10, 20))))

print("4. late events are filtered against the watermark the test set:")
print("  ", t.test("u1", [click("c0", at(8, 0))]), "(08:00 is behind the 10:20 watermark)")

print("\n5. availableNow with timeMode=ProcessingTime: does the query stop by itself?")
from pyspark.sql.streaming import StatefulProcessor


class Count(StatefulProcessor):
    def init(self, handle):
        self.n = handle.getValueState("n", "n LONG")

    def handleInputRows(self, key, rows, timerValues):
        n = (self.n.get()[0] if self.n.exists() else 0) + sum(1 for _ in rows)
        self.n.update((n,))
        yield Row(user=key[0], n=n)

    def close(self):
        pass


w = work("ch41/t4")
(w / "inbox").mkdir()
deliver(w / "inbox", "a.json", '{"user": "a"}\n{"user": "b"}\n')
for mode in ("None", "ProcessingTime"):
    q = (spark.readStream.schema("user STRING").json(str(w / "inbox")).groupBy("user")
         .transformWithState(Count(), "user STRING, n LONG", "Update", mode)
         .writeStream.format("noop").outputMode("update")
         .option("checkpointLocation", str(w / f"ck_{mode}")).trigger(availableNow=True).start())
    t0 = time.time()
    done = q.awaitTermination(45)
    batches = [p.batchId for p in q.recentProgress]
    print(f"   timeMode={mode:<15} finished={done}  after {time.time() - t0:4.1f} s, "
          f"batches {batches[:6]}{' ...' if len(batches) > 6 else ''} ({len(batches)})")
    q.stop()
