"""Step 3 and the box on pandas: the initial state is read only when the checkpoint is new,
and the same tracker written for transformWithStateInPandas.

Run from code/ after t1_spend.py:  uv run python ch26/t5_pandas_restart.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.streaming import StatefulProcessor
from common.book import session, work, ROCKSDB
from checkout import SCHEMA, drop
from common.book import deliver
from t1_spend import SpendTracker

spark = session("ch26-restart", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC",
                spark_sql_streaming_stateStore_providerClass=ROCKSDB)
w = work("ch26/t1", clean=False)                   # t1's inbox and checkpoint
ck = w / "checkpoint"


def total(user):
    return (spark.read.format("statestore").option("stateVarName", "total").load(str(ck))
            .where(F.col("key.user") == user).select("value.amount").first()[0])


print("1. Restart t1's checkpoint with a new opening balance for u1 (500.0) and a new drop:")
print("   u1 total in state before:", total("u1"))
deliver(w / "inbox", "drop_5.jsonl", '{"event_id": "c030", "user": "u1", "action": "pay", "page": "/checkout", '
        '"ts": "2026-10-05T12:05:00Z", "amount": 10.0}\n', mtime=1_000_005)
opening = spark.createDataFrame([("u1", 500.0)], "user STRING, opening DOUBLE")
pays = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "inbox"))
        .where("action = 'pay'").select("user", "event_id", "amount"))
q = (pays.groupBy("user").transformWithState(SpendTracker(), "user STRING, total DOUBLE, payments INT, flagged BOOLEAN",
                                             "Update", "None", initialState=opening.groupBy("user"))
     .writeStream.format("noop").outputMode("update")      # a memory sink in update mode cannot restart
     .option("checkpointLocation", str(ck)).trigger(availableNow=True).start())
q.awaitTermination()
print("   batches this run:", [(p.batchId, p.numInputRows) for p in q.recentProgress])
print("   u1 total in state after: ", total("u1"), "(the 500.0 was not applied)")


class PandasSpend(StatefulProcessor):
    """The same total, but handleInputRows gets pandas DataFrames and yields pandas DataFrames."""

    def init(self, handle):
        self.total = handle.getValueState("total", "amount DOUBLE")

    def handleInputRows(self, key, rows, timerValues):
        total = self.total.get()[0] if self.total.exists() else 0.0
        n = 0
        for pdf in rows:                                 # an iterator of pandas DataFrames
            total += float(pdf["amount"].sum())
            n += len(pdf)
        self.total.update((total,))
        yield pd.DataFrame({"user": [key[0]], "total": [round(total, 2)], "payments": [n]})

    def close(self):
        pass


print("\n2. transformWithStateInPandas, all four drops at once:")
w2 = work("ch26/t5")
(w2 / "inbox").mkdir()
for i in range(1, 5):
    drop(w2 / "inbox", i)
pays = (spark.readStream.schema(SCHEMA).json(str(w2 / "inbox"))
        .where("action = 'pay'").select("user", "event_id", "amount"))
q = (pays.groupBy("user").transformWithStateInPandas(PandasSpend(), "user STRING, total DOUBLE, payments LONG",
                                                      "Update", "None")
     .writeStream.format("memory").queryName("pspend").outputMode("update")
     .option("checkpointLocation", str(w2 / "ck")).trigger(availableNow=True).start())
q.awaitTermination()
spark.table("pspend").orderBy("user").show()
