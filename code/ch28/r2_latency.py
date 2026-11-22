"""Step 4-5: end-to-end latency, Kafka to Kafka, for the same two queries in micro-batch mode and
in Real-time Mode. Latency = CreateTime of the output record - CreateTime of the input record.

Start the broker first:  ch25/kafka.sh start
Run from code/:          uv run python ch28/r2_latency.py [rate] [seconds] [run names ...]
"""
import subprocess
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, KAFKA_PACKAGE, KAFKA_BOOTSTRAP
from producer import produce

RATE = int(sys.argv[1]) if len(sys.argv) > 1 else 200        # records a second
SECONDS = int(sys.argv[2]) if len(sys.argv) > 2 else 40
WARMUP = 10                                                  # seconds of each run left out

spark = session("ch28-latency", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC",
                spark_jars_packages=KAFKA_PACKAGE)
w = work("ch28/r2")
KAFKA = Path(__file__).resolve().parents[1] / "ch25" / "kafka.sh"


def topic(name, partitions):
    subprocess.run([str(KAFKA), "topic", name, str(partitions)], check=True, capture_output=True)


def source(name):
    return (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", name).option("startingOffsets", "latest").load())


def stateless(df):
    """Pass each record through; the output value carries the input CreateTime in ms."""
    return df.select("key", F.concat_ws(",", F.unix_millis("timestamp").cast("string"),
                                        F.col("value").cast("string")).alias("value"))


def stateful(df):
    """A running count per user; the output carries the newest input CreateTime behind the update."""
    return (df.groupBy("key").agg(F.count("*").alias("n"), F.max("timestamp").alias("newest"))
            .select("key", F.concat_ws(",", F.unix_millis("newest").cast("string"),
                                       F.col("n").cast("string")).alias("value")))


def run(label, shape, trigger):
    tin, tout = f"lat_in_{label}", f"lat_out_{label}"
    topic(tin, 4)
    topic(tout, 1)
    q = (shape(source(tin)).writeStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("topic", tout).option("checkpointLocation", str(w / label)).outputMode("update")
         .trigger(**trigger).start())
    while q.status["message"].startswith("Initializing") or not q.recentProgress and "realTime" not in trigger:
        time.sleep(0.5)
    time.sleep(8)                                           # the first batch or the long batch is running
    t0 = int(time.time() * 1000)
    sent = produce(tin, RATE, SECONDS)
    time.sleep(6)
    q.stop()
    out = (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", tout).load()
           .select(F.unix_millis("timestamp").alias("out_ms"),
                   F.split(F.col("value").cast("string"), ",")[0].cast("long").alias("in_ms"))
           .where(F.col("in_ms") >= t0 + WARMUP * 1000)
           .select((F.col("out_ms") - F.col("in_ms")).alias("lat")))
    r = out.agg(F.count("*").alias("n"),
                *[F.percentile("lat", p).alias(f"p{int(p * 100)}") for p in (0.5, 0.9, 0.99)],
                F.max("lat").alias("max")).first()
    print(f"  {label:<20} sent {sent:>5}  out rows {r.n:>5}   p50 {r.p50:>6.0f}  p90 {r.p90:>6.0f}"
          f"  p99 {r.p99:>6.0f}  max {r.max:>6.0f}  ms", flush=True)


print(f"{RATE} records/s for {SECONDS} s into a 4-partition topic, first {WARMUP} s left out, "
      f"local[*] on {spark.sparkContext.defaultParallelism} cores")
RUNS = {"mb_stateless": (stateless, {"processingTime": "0 seconds"}),
        "rtm_stateless": (stateless, {"realTime": "10 seconds"}),
        "mb_count": (stateful, {"processingTime": "0 seconds"}),
        "rtm_count": (stateful, {"realTime": "10 seconds"})}
for label in sys.argv[3:] or RUNS:
    run(label, *RUNS[label])
