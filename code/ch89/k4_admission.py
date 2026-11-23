"""Step 4: how many records a batch takes, and how many tasks read them.

Part 1 splits Kafka partitions into more Spark tasks (minPartitions, maxRecordsPerPartition).
Part 2 makes a stream wait for enough records (minOffsetsPerTrigger, maxTriggerDelay).
Run from code/:  uv run python ch89/k4_admission.py   (after ch89/kafka.sh start)
"""
import threading
import time

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from k89 import spark_session, work, kafka, publish, slices, trip_rows, end_offsets, j, PLAIN

spark = spark_session("ch89-admission")
w = work("ch89/k4")
kafka("topic", "trips4", "4")
lines = trip_rows(slices()[:6])
publish(spark, "trips4", lines)


def read(**opts):
    r = spark.read.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "trips4")
    for k, v in opts.items():
        r = r.option(k, v)
    return r.load()


def tasks(df):
    """One line per Spark partition (= one task): which Kafka partition and which offsets it reads."""
    rows = (df.withColumn("task", F.spark_partition_id()).groupBy("task", "partition")
            .agg(F.min("offset").alias("lo"), F.max("offset").alias("hi"), F.count("*").alias("n"))
            .orderBy("task").collect())
    return rows


print("1. Spark tasks for one batch of the topic (4 Kafka partitions, %d records):" % len(lines))
ends = end_offsets("trips4")
explicit = {"startingOffsets": j({"trips4": {str(p): 0 for p in ends}}),
            "endingOffsets": j({"trips4": {str(p): o for p, o in ends.items()}})}
for label, opts, detail in [
        ("default", {}, True),
        ("minPartitions=10", {"minPartitions": "10"}, True),
        ("maxRecordsPerPartition=400, earliest to latest", {"maxRecordsPerPartition": "400"}, False),
        ("maxRecordsPerPartition=400, explicit offsets", {"maxRecordsPerPartition": "400", **explicit}, True)]:
    df = read(**opts)
    print(f"   {label}: {df.rdd.getNumPartitions()} tasks")
    if detail:
        for r in tasks(df):
            print(f"     task {r.task:>2}  kafka p{r.partition}  offsets {r.lo:>4}-{r.hi:<4} ({r.n} records)")

q = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "trips4")
     .option("startingOffsets", "earliest").option("maxRecordsPerPartition", "400").load()
     .writeStream.format("noop").option("checkpointLocation", str(w / "split")).trigger(availableNow=True).start())
q.awaitTermination()
from common.book import ui_api
scan = [s for s in ui_api("stages?status=complete") if s["numTasks"] > 1]
print(f"   the same option on a stream: the batch ran {scan[0]['numTasks']} tasks (Spark UI REST API)")

print("\n2. A stream that waits for 1000 records, or at most 6 seconds after the last batch:")
kafka("topic", "drip", "2")
t0 = time.time()
clock = lambda: f"{time.time() - t0:5.1f}s"


class Printer(StreamingQueryListener):
    def onQueryStarted(self, e): pass
    def onQueryIdle(self, e): pass
    def onQueryTerminated(self, e): pass

    def onQueryProgress(self, e):
        p = e.progress
        if p.numInputRows:
            print(f"   {clock()}  batch {p.batchId}  rows {p.numInputRows:>4}", flush=True)


spark.streams.addListener(Printer())
q = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "drip")
     .option("startingOffsets", "earliest")
     .option("minOffsetsPerTrigger", 1000).option("maxTriggerDelay", "6s").load()
     .writeStream.format("noop").option("checkpointLocation", str(w / "drip"))
     .trigger(processingTime="1 second").start())


def drip():
    pos = 0
    for n, pause in [(400, 1.5)] * 5 + [(100, 2)] * 6:   # fast at first, then slow
        publish(spark, "drip", lines[pos:pos + n])
        pos += n
        print(f"   {clock()}  + {n} records", flush=True)
        time.sleep(pause)


threading.Thread(target=drip).start()
time.sleep(28)
q.stop()
print("   stopped")
spark.stop()
