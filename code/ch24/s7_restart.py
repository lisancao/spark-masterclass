"""Step 7: kill a running query, then restart it from its checkpoint.

  uv run python ch24/s7_restart.py crash    # feeds files, dies inside batch 3
  uv run python ch24/s7_restart.py resume   # same checkpoint, shuffle partitions set to 8
"""
import os
import sys
import threading
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver
from replay import slices, SCHEMA

MODE = sys.argv[1]
w = work("ch24/s7-restart", clean=(MODE == "crash"))
inbox, ckpt, out = w / "inbox", w / "checkpoint", w / "out"
inbox.mkdir(exist_ok=True)
parts = "4" if MODE == "crash" else "8"
spark = session("ch24-restart", spark_sql_shuffle_partitions=parts, spark_sql_session_timeZone="UTC")
files = slices()


def ids(folder):
    d = ckpt / folder
    return sorted(int(f.name) for f in d.iterdir() if f.name.isdigit()) if d.exists() else []


if MODE == "resume":
    print(f"offsets {ids('offsets')}, commits {ids('commits')}: batch {ids('offsets')[-1]} "
          f"was planned and never committed, so it runs again")
    print(f"spark.sql.shuffle.partitions is now {spark.conf.get('spark.sql.shuffle.partitions')}")


def sink(df, batch_id):
    df.write.mode("overwrite").parquet(str(out / f"batch_id={batch_id}"))   # a replay replaces its batch
    midtown = df.where("PULocationID = 161").collect()
    total = midtown[0]["trips"] if midtown else "-"
    print(f"  batch {batch_id}: zones updated {df.count():>3}, Midtown Center trips so far {total}")
    if MODE == "crash" and batch_id == 3:
        print("  ** the driver dies inside batch 3 (os._exit) **", flush=True)
        os._exit(1)


trips = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(inbox))
q = (trips.groupBy("PULocationID").agg(F.count("*").alias("trips"))
     .writeStream.foreachBatch(sink).outputMode("update")
     .option("checkpointLocation", str(ckpt))
     .trigger(processingTime="1 second").start())

todo = files[:6] if MODE == "crash" else files[4:6]
for k, (name, text, n) in enumerate(todo):
    deliver(inbox, name, text)
    time.sleep(2)
# Wait for the work, not for a fixed time: crash mode dies inside batch 3, resume mode ends at batch 5.
deadline = time.time() + 180
while time.time() < deadline and not (MODE == "resume" and 5 in ids("commits")):
    time.sleep(0.5)
for p in q.recentProgress:
    if p.numInputRows:
        op = p.stateOperators[0]
        print(f"batch {p.batchId}: numShufflePartitions {op.numShufflePartitions}, "
              f"numStateStoreInstances {op.numStateStoreInstances}")
q.stop()
print(f"offsets {ids('offsets')}, commits {ids('commits')}")
