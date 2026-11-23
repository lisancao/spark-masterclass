"""Step 7: what a checkpoint holds, file by file, after two runs of a windowed count.

Run from code/:  uv run python ch48/s7_checkpoint.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver, tree
from replay import slices, SCHEMA

spark = session("ch48-checkpoint", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC")
w = work("ch48/s7")
inbox, ckpt, out = w / "inbox", w / "checkpoint", w / "out"
inbox.mkdir()
files = slices()


def run():
    trips = spark.readStream.schema(SCHEMA).json(str(inbox))
    q = (trips.withWatermark("pickup_ts", "15 minutes")
         .groupBy(F.window("pickup_ts", "10 minutes").alias("w")).count()
         .select(F.col("w.start").alias("start"), "count")
         .writeStream.format("parquet").option("path", str(out))
         .outputMode("append")
         .option("checkpointLocation", str(ckpt))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    return [(p.batchId, p.numInputRows) for p in q.recentProgress]


deliver(inbox, *files[0][:2])
print("run 1 batches (id, rows):", run())
deliver(inbox, *files[1][:2])
print("run 2 batches (id, rows):", run())

print("\nThe checkpoint (.crc checksum files left out):")
print(tree(ckpt))


def show(rel):
    print(f"\n--- {rel}")
    print((ckpt / rel).read_text().rstrip())


show("metadata")
import json, datetime as dt
lines = (ckpt / "offsets/2").read_text().splitlines()
meta = json.loads(lines[1])
print("\n--- offsets/2 (line 2 pretty-printed)")
print(lines[0])
print(json.dumps(meta, indent=1))
print("\n".join(lines[2:]))
utc = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
print(f"batchWatermarkMs = {utc(meta['batchWatermarkMs'])} UTC")
show("commits/2")
print(f"nextBatchWatermarkMs = {utc(json.loads((ckpt / 'commits/2').read_text().splitlines()[1])['nextBatchWatermarkMs'])} UTC")
show("sources/0/1")
print("\nThe sink's own log, next to the Parquet files:")
print(tree(out / "_spark_metadata"))
print((out / "_spark_metadata" / "2").read_text().rstrip().replace(str(Path.home()), "~"))
