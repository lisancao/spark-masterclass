"""Step 9: the same trips through Kafka. A batch job publishes them to a 4-partition topic;
a streaming query reads the topic, and its checkpoint records one offset per partition.

Start the broker first:  ch39/kafka.sh start && ch39/kafka.sh topic trips 4
Run from code/:          uv run python ch39/s9_kafka.py
"""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, KAFKA_PACKAGE, KAFKA_BOOTSTRAP
from replay import slices, SCHEMA

spark = session("ch39-kafka", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC",
                spark_jars_packages=KAFKA_PACKAGE)
w = work("ch39/s9")
files = slices()


def publish(chunk):
    """A batch write to Kafka: the key picks the partition, the value is the trip as JSON."""
    lines = [l for _, text, _ in chunk for l in text.splitlines()]
    df = spark.createDataFrame([(l,) for l in lines], "value string")
    trip = F.from_json("value", SCHEMA)
    (df.select(trip.PULocationID.cast("string").alias("key"), "value")
       .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("topic", "trips").save())
    return len(lines)


print("published", publish(files[:6]), "trips (dropoffs 08:00 to 08:30)")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", "trips")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 1200)
       .load())
print("\nWhat the Kafka source gives you:")
raw.printSchema()

trips = raw.select("partition", "offset", F.from_json(F.col("value").cast("string"), SCHEMA).alias("t")).select("partition", "offset", "t.*")


def run():
    q = (trips.groupBy("partition").agg(F.count("*").alias("trips"), F.max("offset").alias("last_offset"))
         .writeStream.format("memory").queryName("by_partition").outputMode("complete")
         .option("checkpointLocation", str(w / "checkpoint"))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    return q


q = run()
print("run 1 batches (id, rows):", [(p.batchId, p.numInputRows) for p in q.recentProgress])
spark.table("by_partition").orderBy("partition").show()
last = max(int(f.name) for f in (w / "checkpoint/offsets").iterdir() if f.name.isdigit())
print(f"offsets/{last}, line 3 (the source's offset: next offset to read, per partition):")
print("  " + (w / f"checkpoint/offsets/{last}").read_text().splitlines()[2])
src = q.recentProgress[-1].sources[0]
print("progress, the same source:", json.dumps({"endOffset": json.loads(src.endOffset),
                                                  "latestOffset": json.loads(src.latestOffset)}))

print("\npublished", publish(files[6:]), "more trips (08:30 to 09:00)")
q = run()
print("run 2 batches (id, rows):", [(p.batchId, p.numInputRows) for p in q.recentProgress])
spark.table("by_partition").orderBy("partition").show()
