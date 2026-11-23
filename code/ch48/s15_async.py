"""Step 15: asynchronous progress tracking. The offset log and the commit log leave the
critical path of each micro-batch.

A stateless Kafka-to-Kafka query copies a topic in small batches (maxOffsetsPerTrigger=100),
first with the default progress tracking and then with asyncProgressTrackingEnabled=true.
Start a broker first:  ch48/kafka.sh start   (or set KAFKA_CONTAINER and KAFKA_BOOTSTRAP)
Run from code/:        flock ~/spark-masterclass/data/.bench.lock uv run python ch48/s15_async.py
"""
import statistics
import subprocess
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, error_text, KAFKA_PACKAGE, KAFKA_BOOTSTRAP
from replay import slices

HERE = Path(__file__).resolve().parent
spark = session("ch48-async", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC",
                spark_jars_packages=KAFKA_PACKAGE, spark_ui_showConsoleProgress="false")
w = work("ch48/s15")
for t in ("apt_in", "apt_out"):
    subprocess.run([str(HERE / "kafka.sh"), "topic", t, "4"], check=True, capture_output=True)
lines = [l for _, text, _ in slices() for l in text.splitlines()]
(spark.createDataFrame([(l,) for l in lines], "value string")
 .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("topic", "apt_in").save())
print(f"published {len(lines)} trips to apt_in (4 partitions)")

src = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", "apt_in").option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 100).load()
       .select("key", "value"))


def copy(name, df=src, sink="kafka", trigger=None, **opts):
    wr = df.writeStream.option("checkpointLocation", str(w / name))
    if sink == "kafka":
        wr = wr.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("topic", "apt_out")
    elif sink == "foreachBatch":
        wr = wr.foreachBatch(lambda d, b: None)
    else:
        wr = wr.format(sink).option("path", str(w / f"{name}_out"))
    for k, v in opts.items():
        wr = wr.option(k, v)
    if trigger:
        wr = wr.trigger(**trigger)
    return wr.start()


def measure(name, **opts):
    t = time.time()
    q = copy(name, **opts)
    q.processAllAvailable()
    wall = time.time() - t
    q.stop()
    ps = [p for p in q.recentProgress if p.numInputRows]
    d = lambda k: statistics.median(p.durationMs.get(k, 0) for p in ps)
    logs = {f: len([x for x in (w / name / f).iterdir() if x.name.isdigit()]) for f in ("offsets", "commits")}
    return dict(batches=len(ps), wall=wall, wal=d("walCommit"), commit=d("commitOffsets"),
                trig=d("triggerExecution"), **logs)


print("\n1. The same copy, synchronous and asynchronous progress tracking (3 runs each, alternated):")
runs = {"sync": [], "async": []}
measure("warmup")
for i in range(3):
    runs["sync"].append(measure(f"sync{i}"))
    runs["async"].append(measure(f"async{i}", asyncProgressTrackingEnabled="true"))
print("mode    batches  offsets/ files  commits/ files  walCommit  commitOffsets  batch (ms)  wall (s)")
for mode, rs in runs.items():
    m = lambda k: statistics.median(r[k] for r in rs)
    print(f"{mode:<7} {m('batches'):>7.0f}  {m('offsets'):>14.0f}  {m('commits'):>14.0f}  {m('wal'):>9.0f}  "
          f"{m('commit'):>13.0f}  {m('trig'):>10.0f}  {m('wall'):>8.1f}")
print("(medians over the runs; walCommit, commitOffsets and batch are medians per run of durationMs)")

print("\n2. What async progress tracking does not accept:")
cases = [("trigger availableNow", dict(trigger={"availableNow": True})),
         ("a stateful query", dict(df=src.groupBy("key").count(), sink="kafka")),
         ("foreachBatch sink", dict(sink="foreachBatch")),
         ("Parquet sink", dict(sink="parquet"))]
for label, kw in cases:
    df = kw.pop("df", src)
    if label == "a stateful query":
        df = df.select(F.col("key"), F.col("count").cast("string").alias("value"))
        kw["outputMode"] = "update"
    mode = kw.pop("outputMode", None)
    try:
        if mode:
            q = (df.writeStream.outputMode(mode).format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                 .option("topic", "apt_out").option("checkpointLocation", str(w / "bad_state"))
                 .option("asyncProgressTrackingEnabled", "true").start())
        else:
            q = copy("bad_" + label.split()[0], df=df, asyncProgressTrackingEnabled="true", **kw)
        q.processAllAvailable()
        q.stop()
        print(f"   {label:<22} accepted")
    except Exception as e:
        print(f"   {label}:\n" + error_text(e, 84, 2))

print("\n3. Turn it off on a checkpoint that it wrote:")
ck = "async0"
print("   offsets/ of the async run:", sorted(int(x.name) for x in (w / ck / "offsets").iterdir() if x.name.isdigit())[-4:],
      " commits/:", sorted(int(x.name) for x in (w / ck / "commits").iterdir() if x.name.isdigit())[-4:])
subprocess.run(["true"])
(spark.createDataFrame([(l,) for l in lines[:500]], "value string")
 .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("topic", "apt_in").save())
try:
    q = copy(ck)                                             # the same checkpoint, async off
    q.processAllAvailable()
    q.stop()
    print("   restart with async off: ran", [(p.batchId, p.numInputRows) for p in q.recentProgress if p.numInputRows][:3])
except Exception as e:
    print("   restart with async off:" + error_text(e, 80, 2).strip())
q = copy(ck, asyncProgressTrackingEnabled="true", asyncProgressTrackingCheckpointIntervalMs="0")
q.processAllAvailable()
q.stop()
print("   the documented fix: async on with asyncProgressTrackingCheckpointIntervalMs=0, batches",
      [p.batchId for p in q.recentProgress if p.numInputRows][:3], "...")
(spark.createDataFrame([(l,) for l in lines[500:700]], "value string")
 .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("topic", "apt_in").save())
q = copy(ck)
q.processAllAvailable()
q.stop()
print("   then async off again: batches", [(p.batchId, p.numInputRows) for p in q.recentProgress if p.numInputRows])
spark.stop()
