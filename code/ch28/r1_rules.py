"""Step 1-2: which queries Real-time Mode accepts in Spark 4.3. Each probe starts a query with
trigger(realTime=...) and prints the error condition, or that it ran.

Start the broker first:  ch25/kafka.sh start && ch25/kafka.sh topic rtm_in 4
Run from code/:          uv run python ch28/r1_rules.py
"""
import re
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Row, functions as F
from pyspark.sql.streaming import StatefulProcessor
from common.book import session, work, KAFKA_PACKAGE, KAFKA_BOOTSTRAP

spark = session("ch28-rules", spark_sql_shuffle_partitions="2", spark_jars_packages=KAFKA_PACKAGE)
w = work("ch28/r1")


def kafka():
    return (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", "rtm_in").load()
            .select(F.col("key").cast("string").alias("k"), F.col("value").cast("string").alias("v"), "timestamp"))


class Count(StatefulProcessor):
    def init(self, handle):
        self.n = handle.getValueState("n", "n LONG")

    def handleInputRows(self, key, rows, timerValues):
        n = (self.n.get()[0] if self.n.exists() else 0) + sum(1 for _ in rows)
        self.n.update((n,))
        yield Row(k=key[0], n=n)

    def close(self):
        pass


def console(df, mode="update"):
    return df.writeStream.format("console").outputMode(mode)


probes = {
    "Kafka -> console, stateless":       lambda: console(kafka().where("v IS NOT NULL")),
    "Kafka -> console, groupBy count":   lambda: console(kafka().groupBy("k").count()),
    "Kafka -> console, dropDuplicates":  lambda: console(kafka().dropDuplicates(["v"])),
    "Kafka -> memory sink":              lambda: kafka().writeStream.format("memory").queryName("m").outputMode("update"),
    "Kafka -> console, append mode":     lambda: console(kafka(), "append"),
    "rate source -> console":            lambda: console(spark.readStream.format("rate").load()),
    "Kafka -> foreachBatch":             lambda: kafka().writeStream.foreachBatch(lambda d, i: None).outputMode("update"),
    "Kafka -> Python UDF -> console":    lambda: console(kafka().select(F.udf(lambda s: s, "string")("v"))),
    "Kafka -> PySpark transformWithState": lambda: console(kafka().groupBy("k").transformWithState(
                                              Count(), "k STRING, n LONG", "Update", "None")),
    "Kafka -> repartitionByRange":       lambda: console(kafka().repartitionByRange(2, "k")),
    "stream-stream join":                lambda: console(kafka().alias("a").withWatermark("timestamp", "1 minute").join(
                                              kafka().alias("b").withWatermark("timestamp", "1 minute"),
                                              F.expr("a.k = b.k AND b.timestamp BETWEEN a.timestamp AND a.timestamp + INTERVAL 1 MINUTE"))),
}


def condition(e):
    m = re.findall(r"\[([A-Z_]+(?:\.[A-Z_0-9]+)?)\]", str(e))
    m = [c for c in m if c != "STREAM_FAILED"]
    return m[0] if m else str(e).split("\n")[0][:80]


def probe(name, build, trigger="5 seconds", conf=None):
    for k, v in (conf or {}).items():
        spark.conf.set(k, v)
    try:
        q = (build().option("checkpointLocation", str(w / re.sub(r"\W+", "_", name)))
             .trigger(realTime=trigger).start())
        t0 = time.time()
        while time.time() - t0 < 12 and q.isActive and q.exception() is None:
            time.sleep(0.5)
        err = q.exception()
        q.stop()
        print(f"  {name:<38} {'ran' if err is None else condition(err) + '  (from the running query)'}")
    except Exception as e:
        print(f"  {name:<38} {condition(e)}")
    finally:
        for k in (conf or {}):
            spark.conf.unset(k)


print("Each probe runs for up to 12 s with trigger(realTime='5 seconds'):")
for name, build in probes.items():
    probe(name, build)
print("\nThe trigger and the configuration:")
probe("trigger(realTime='1 second')", probes["Kafka -> console, stateless"], trigger="1 second")
probe("HDFS state store set explicitly", probes["Kafka -> console, groupBy count"],
      conf={"spark.sql.streaming.stateStore.providerClass":
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"})
probe("checkpointFormatVersion=1", probes["Kafka -> console, groupBy count"],
      conf={"spark.sql.streaming.stateStore.checkpointFormatVersion": "1"})
