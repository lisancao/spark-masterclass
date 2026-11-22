"""Step 1-3: a first StatefulProcessor. Spend per user from the checkout payments, with a
ValueState (the total), a ListState (every payment), initial state (opening balances),
and the state read back from the checkpoint.

Run from code/:  uv run python ch26/t1_spend.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Row, functions as F
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from common.book import session, work, error_text, ROCKSDB
from checkout import drop, SCHEMA

LIMIT = 50.0


class SpendTracker(StatefulProcessor):
    """Total spend per user; flags a user once the total passes LIMIT."""

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.total = handle.getValueState("total", "amount DOUBLE")
        self.payments = handle.getListState("payments", "event_id STRING, amount DOUBLE")

    def handleInitialState(self, key, initialState, timerValues) -> None:
        self.total.update((initialState.opening,))       # once per key, before any input

    def handleInputRows(self, key, rows, timerValues):
        (user,) = key
        total = self.total.get()[0] if self.total.exists() else 0.0
        n = 0
        for r in rows:
            total += r.amount
            n += 1
            self.payments.appendValue((r.event_id, r.amount))
        self.total.update((total,))
        yield Row(user=user, total=round(total, 2), payments=n, flagged=total > LIMIT)

    def close(self) -> None:
        pass


def build(spark, inbox):
    pays = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(inbox))
            .where("action = 'pay'").select("user", "event_id", "amount"))
    opening = spark.createDataFrame([("u1", 20.0), ("u5", 5.0)], "user STRING, opening DOUBLE")
    return pays.groupBy("user").transformWithState(
        statefulProcessor=SpendTracker(),
        outputStructType="user STRING, total DOUBLE, payments INT, flagged BOOLEAN",
        outputMode="Update",
        timeMode="None",
        initialState=opening.groupBy("user"))


spark = session("ch26-spend", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC")
w = work("ch26/t1")
inbox = w / "inbox"
inbox.mkdir()
for i in range(1, 5):
    drop(inbox, i)

print("1. On the default state store:")
try:
    q = (build(spark, inbox).writeStream.format("memory").queryName("nope").outputMode("update")
         .option("checkpointLocation", str(w / "ck_hdfs")).trigger(availableNow=True).start())
    q.awaitTermination()
except Exception as e:
    print(error_text(e, lines=4))

spark.conf.set("spark.sql.streaming.stateStore.providerClass", ROCKSDB)
ck = w / "checkpoint"
q = (build(spark, inbox).writeStream.format("memory").queryName("spend").outputMode("update")
     .option("checkpointLocation", str(ck)).trigger(availableNow=True).start())
q.awaitTermination()
print("\n2. With RocksDB. Batches (id, input rows):", [(p.batchId, p.numInputRows) for p in q.recentProgress])
print("Every row the memory sink received (update mode appends each emitted row):")
spark.table("spend").orderBy("user", "total").show()
print("The latest row per user:")
spark.sql("""SELECT user, total, flagged FROM spend
             QUALIFY row_number() OVER (PARTITION BY user ORDER BY total DESC) = 1
             ORDER BY user""").show()

print("3. The same state, read from the checkpoint with the state data source")
spark.read.format("statestore").option("stateVarName", "total").load(str(ck)) \
    .select("key.user", F.col("value.amount").alias("total"), "partition_id").orderBy("user").show()
spark.read.format("statestore").option("stateVarName", "payments").load(str(ck)) \
    .select("key.user", "list_element.*").orderBy("user", "event_id").show()
print("state-metadata: what the checkpoint says each operator keeps")
spark.read.format("state-metadata").load(str(ck)) \
    .select("operatorName", "stateStoreName", "numPartitions", "minBatchId", "maxBatchId").show(truncate=False)
