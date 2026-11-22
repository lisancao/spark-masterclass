"""The Sessionizer of Step 4, shared by t2_sessions.py and t4_tester.py."""
from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessor

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
