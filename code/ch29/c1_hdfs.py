"""Steps 1-3: the HDFS-backed state store on disk. A per-user count over the four checkout drops,
then the delta files as bytes, decoded with the standard library, a snapshot, and the same
state through the state data source (current, as of an old batch, and as a change feed).

Run from code/:  uv run python ch29/c1_hdfs.py
"""
import subprocess
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ch27"))

from pyspark.sql import functions as F
from common.book import session, work, tree
from checkout import drop, SCHEMA
from lz4delta import lz4_java_stream, records, unsafe_string, unsafe_long

spark = session("ch29-hdfs", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC",
                spark_sql_streaming_stateStore_minDeltasForSnapshot="2",      # default 10
                spark_sql_streaming_stateStore_maintenanceInterval="1s")      # default 60s
w = work("ch29/c1")
inbox, ck = w / "inbox", w / "checkpoint"
inbox.mkdir()

counts = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(inbox))
          .dropDuplicates(["event_id"]).groupBy("user").count())
for i in range(1, 5):
    drop(inbox, i)
q = (counts.writeStream.format("noop").outputMode("update").option("checkpointLocation", str(ck))
     .trigger(processingTime="1 second").start())
while len([p for p in q.recentProgress if p.numInputRows > 0]) < 4:
    time.sleep(0.5)
time.sleep(4)                                    # let the maintenance thread write snapshots
q.stop()
print("batches (id, input rows):", [(p.batchId, p.numInputRows) for p in q.recentProgress][:6])
print("operators:", [s.operatorName for s in q.lastProgress.stateOperators])

print("\nThe state of operator 0 (the count), .crc files left out:")
print(tree(ck / "state" / "0"))

d = ck / "state" / "0" / "0" / "1.delta"
print(f"\n$ xxd {d.relative_to(ck)}")
print(subprocess.run(["xxd", str(d)], capture_output=True, text=True).stdout.rstrip())

print("\nEach delta and snapshot of the count, decoded (key: user, value: count):")
for f in sorted((ck / "state" / "0").glob("*/[0-9]*.*"), key=lambda p: (p.parent.name, int(p.stem), p.suffix)):
    if f.suffix not in (".delta", ".snapshot"):
        continue
    raw = lz4_java_stream(f.read_bytes())
    rows = [(unsafe_string(k, 0, 1), None if v is None else unsafe_long(v, 0, 1)) for k, v in records(raw)]
    print(f"  {f.parent.name}/{f.name:<11} {len(raw):>4} bytes  {rows}")

print("\nThe same store through the state data source:")
spark.read.format("statestore").option("operatorId", 0).load(str(ck)) \
    .selectExpr("key.user", "value.count", "partition_id").orderBy("user").show()
print("As of batch 1 (option batchId):")
spark.read.format("statestore").option("operatorId", 0).option("batchId", 1).load(str(ck)) \
    .selectExpr("key.user", "value.count").orderBy("user").show()
print("The change feed of partition 0:")
spark.read.format("statestore").option("operatorId", 0).option("readChangeFeed", "true") \
    .option("changeStartBatchId", 0).load(str(ck)).where("partition_id = 0") \
    .selectExpr("batch_id", "change_type", "key.user", "value.count").orderBy("batch_id", "user").show()
print("Operator metadata:")
spark.read.format("state-metadata").load(str(ck)) \
    .select("operatorId", "operatorName", "numPartitions", "minBatchId", "maxBatchId").show(truncate=False)
print("--- state/0/_metadata/metadata")
print((ck / "state/0/_metadata/metadata").read_text())
print("--- state/0/0/_metadata/schema (printable text only: a version, the key schema, the value schema)")
import re
for part in re.findall(r"[ -~]{2,}", (ck / "state/0/0/_metadata/schema").read_bytes().decode("latin-1")):
    print(part.strip())
