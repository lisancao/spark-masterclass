"""Step 8: change the value schema of a transformWithState variable and restart, under each
state encoding. The first run stores n=1 for one key; the second run reads the value back with
the new schema and prints what it got, or the error condition.

Run from code/:  uv run python ch43/c4_evolution.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ch41"))

from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessor
from common.book import session, work, deliver, ROCKSDB
import checkout  # noqa: F401  (puts ch41/ on the workers' path)

spark = session("ch43-evolution", spark_sql_shuffle_partitions="1", spark_sql_streaming_stateStore_providerClass=ROCKSDB)


def processor(schema, initial):
    class P(StatefulProcessor):
        def init(self, handle):
            self.v = handle.getValueState("v", schema)

        def handleInputRows(self, key, rows, timerValues):
            for _ in rows:
                pass
            before = self.v.get() if self.v.exists() else None
            if before is None:
                self.v.update(initial)
            yield Row(k=key[0], before=str(before))

        def close(self):
            pass
    return P()


def run(ck, inbox, schema, initial):
    q = (spark.readStream.schema("k STRING").json(str(inbox)).groupBy("k")
         .transformWithState(processor(schema, initial), "k STRING, before STRING", "Update", "None")
         .writeStream.foreachBatch(lambda df, _: seen.extend(df.collect())).outputMode("update")
         .option("checkpointLocation", str(ck)).trigger(availableNow=True).start())
    seen = []
    q.awaitTermination()
    return seen[-1].before


CASES = [
    ("no change",            "n INT",              "n INT"),
    ("rename n to count",    "n INT",              "count INT"),
    ("add a field",          "n INT",              "n INT, note STRING"),
    ("widen INT to BIGINT",  "n INT",              "n BIGINT"),
    ("drop a field",         "n INT, note STRING", "n INT"),
    ("narrow BIGINT to INT", "n BIGINT",           "n INT"),
]
first = {"n INT": (1,), "n INT, note STRING": (1, "x"), "n BIGINT": (1,)}

print(f"{'change':<22} {'unsaferow':<44} avro")
for name, old, new in CASES:
    cells = []
    for enc in ("unsaferow", "avro"):
        spark.conf.set("spark.sql.streaming.stateStore.encodingFormat", enc)
        w = work(f"ch43/c4/{enc}_{re.sub(r'\\W+', '_', name)}")
        (w / "inbox").mkdir()
        deliver(w / "inbox", "1.json", '{"k": "a"}\n', mtime=1)
        run(w / "ck", w / "inbox", old, first[old])
        deliver(w / "inbox", "2.json", '{"k": "a"}\n', mtime=2)
        try:
            cells.append("read " + run(w / "ck", w / "inbox", new, (0,) * len(new.split(","))))
        except Exception as e:
            m = re.findall(r"\[([A-Z_]{12,}(?:\.[A-Z_]+)?)\]", str(e))
            cells.append([c for c in m if c != "STREAM_FAILED"][0] if m else str(e)[:40])
    print(f"{name:<22} {cells[0]:<44} {cells[1]}")
