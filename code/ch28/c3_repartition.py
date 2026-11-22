"""Step 7: change the number of state partitions of a stopped query, offline.
spark._streamingCheckpointManager.repartition (experimental, Spark 4.2+, Classic only).

Run from code/:  uv run python ch28/c3_repartition.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ch26"))

from common.book import session, work, error_text, ROCKSDB
from checkout import drop, SCHEMA

spark = session("ch28-repartition", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC",
                spark_sql_streaming_stateStore_providerClass=ROCKSDB)
w = work("ch28/c3")
inbox, ck = w / "inbox", w / "checkpoint"
inbox.mkdir()


def run():
    q = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(inbox))
         .dropDuplicates(["event_id"]).groupBy("user").count()
         .writeStream.format("noop").outputMode("update")
         .option("checkpointLocation", str(ck)).trigger(availableNow=True).start())
    q.awaitTermination()
    return [(p.batchId, p.numInputRows) for p in q.recentProgress]


def state():
    df = spark.read.format("statestore").option("operatorId", 0).load(str(ck))
    rows = df.selectExpr("key.user", "value.count", "partition_id").orderBy("user").collect()
    return " ".join(f"{r.user}={r['count']}@p{r.partition_id}" for r in rows)


def parts():
    return sorted(p.name for p in (ck / "state" / "0").iterdir() if p.name.isdigit())


for i in (1, 2):
    drop(inbox, i)
print("1. two drops on 2 partitions. batches:", run())
print("   state:", state())
print("   state/0 folders:", parts())

print("\n2. repartition the stopped query to 4 partitions")
spark._streamingCheckpointManager.repartition(str(ck), 4)
print("   offsets and commits now:", sorted(p.name for p in (ck / "offsets").iterdir() if p.name.isdigit()),
      sorted(p.name for p in (ck / "commits").iterdir() if p.name.isdigit()))
print("   state/0 folders:", parts())
print("   state:", state())

print("\n3. restart with the last two drops; the session still says 2 partitions")
for i in (3, 4):
    drop(inbox, i)
print("   batches:", run())
print("   state:", state())

print("\n4. the same call on the default (HDFS) state store")
spark.conf.unset("spark.sql.streaming.stateStore.providerClass")
w2 = work("ch28/c3_hdfs")
(w2 / "inbox").mkdir()
drop(w2 / "inbox", 1)
q = (spark.readStream.schema(SCHEMA).json(str(w2 / "inbox")).groupBy("user").count()
     .writeStream.format("noop").outputMode("update").option("checkpointLocation", str(w2 / "ck"))
     .trigger(availableNow=True).start())
q.awaitTermination()
try:
    spark._streamingCheckpointManager.repartition(str(w2 / "ck"), 4)
except Exception as e:
    import re, textwrap
    m = re.search(r"\[STATE_REPARTITION[^\n]*", str(e))
    print(textwrap.indent(textwrap.fill(re.sub(r"'file:[^']*'", "'.../ck'", m.group(0)), 90), "  ") if m else error_text(e))
