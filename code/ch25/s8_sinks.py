"""Step 8: sinks, admission control and the edges of the API, each shown with its real result.

Run from code/:  uv run python ch25/s8_sinks.py
"""
import sys
import time
import warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver, error_text
from replay import slices, SCHEMA

spark = session("ch25-sinks", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC")
w = work("ch25/s8")
inbox = w / "inbox"
inbox.mkdir()
for k, (name, text, n) in enumerate(slices()[:4]):
    deliver(inbox, name, text, mtime=1_000_000 + k)
trips = lambda **o: spark.readStream.schema(SCHEMA).options(**o).json(str(inbox))
per_zone = lambda df: df.groupBy("PULocationID").agg(F.count("*").alias("trips"))


def start(df, label, mode="update", **kw):
    wr = df.writeStream.outputMode(mode).option("checkpointLocation", str(w / label))
    fmt = kw.pop("format", None)
    fe = kw.pop("foreachBatch", None)
    wr = wr.foreachBatch(fe) if fe else wr.format(fmt)
    for k, v in kw.items():
        wr = wr.option(k, v)
    q = wr.trigger(availableNow=True).start()
    q.awaitTermination()
    return q


print("1. Admission control: how many rows each micro-batch takes")
for opts in ({}, {"maxFilesPerTrigger": 1}, {"maxFilesPerTrigger": 3}, {"maxBytesPerTrigger": 200_000}):
    q = start(trips(**opts).select("trip_id"), f"adm{len(opts)}{list(opts.values())}", mode="append",
              format="noop")
    print(f"   {str(opts) or '{}':<30} rows per batch {[p.numInputRows for p in q.recentProgress]}")
try:
    start(trips(maxFilesPerTrigger=1, maxBytesPerTrigger=1000).select("trip_id"), "both", "append", format="noop")
except Exception as e:
    print(error_text(e, lines=2))
q = start(trips(maxFilesPerTrigger=1, latestFirst="true").select(F.col("_metadata.file_name")),
          "latest", "append", format="memory", queryName="order")
print(f"   {'latestFirst + maxFiles 1':<30} rows per batch {[p.numInputRows for p in q.recentProgress]}"
      " (newest file first)")

print("\n2. foreachBatch runs the batch's plan once per action on it")
for persist in (False, True):
    def body(df, batch_id, persist=persist):
        if persist:
            df = df.persist()
        df.write.mode("overwrite").parquet(str(w / f"fb{persist}/batch_id={batch_id}"))
        df.count()
        df.where("trips > 5").collect()
        if persist:
            df.unpersist()
    q = start(per_zone(trips(maxFilesPerTrigger=4)), f"fb{persist}", foreachBatch=body)
    op = q.recentProgress[0].stateOperators[0]
    print(f"   persist={persist!s:<5}  shuffle partitions {op.numShufflePartitions}, "
          f"state store instances loaded {op.numStateStoreInstances}")

print("\n3. The memory sink keeps nothing across restarts:")
start(trips().select("trip_id"), "mem", "append", format="memory", queryName="mem")
try:
    start(trips().select("trip_id"), "mem", "append", format="memory", queryName="mem")
except Exception as e:
    print(error_text(e, lines=2))

print("\n4. A file sink needs a checkpoint location of its own:")
try:
    trips().writeStream.format("parquet").option("path", str(w / "nockpt")).start()
except Exception as e:
    print(error_text(e, lines=2))

print("\n5. trigger(once=True), which the 4.3 guide marks deprecated:")
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    q = (trips().select("trip_id").writeStream.format("noop")
         .option("checkpointLocation", str(w / "once")).trigger(once=True).start())
    q.awaitTermination()
print("  ", [str(c.message)[:80] for c in caught] or "no Python warning",
      "| batches:", [p.numInputRows for p in q.recentProgress])

print("\n6. A Real-time trigger on a file source:")
try:
    q = (trips().select("trip_id").writeStream.format("console").outputMode("update")
         .option("checkpointLocation", str(w / "rtm")).trigger(realTime="5 seconds").start())
    q.awaitTermination(30)
except Exception as e:
    print(error_text(e, lines=3))
