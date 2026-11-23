"""Step 7: the Kafka sink writes at least once. A replayed batch writes its records again.

A stream copies the topic `rides` to the topic `rides_out` with the Kafka sink. Then the script
removes commits/2 from the checkpoint. That is the state a crash leaves after the sink wrote
batch 2 and before Spark wrote the commit (Chapter 48, Step 7). The restart runs batch 2 again.
Run from code/:  uv run python ch53/k7_sink.py   (after ch53/kafka.sh start)
"""
from pyspark.sql import functions as F
from k89 import spark_session, work, kafka, publish, slices, trip_rows, err, PLAIN, SCHEMA

spark = spark_session("ch53-sink")
w = work("ch53/k7")
kafka("topic", "rides", "3")
kafka("topic", "rides_out", "3")
n_in = publish(spark, "rides", trip_rows(slices()[:3]), key="trip_id")
print(f"published {n_in} trips to rides")

src = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "rides")
       .option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 500).load())
t = F.from_json(F.col("value").cast("string"), SCHEMA)
out = src.select(
    F.col("key"),                                                     # trip_id, a business key
    F.to_json(F.struct(t.trip_id, t.PULocationID, t.total_amount)).alias("value"),
    F.array(F.struct(F.lit("src").alias("key"),                       # where the record came from
                     F.concat_ws(":", "topic", "partition", "offset").cast("binary").alias("value"))).alias("headers"))


def run():
    q = (out.writeStream.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("topic", "rides_out")
         .option("checkpointLocation", str(w / "ck")).trigger(availableNow=True).start())
    q.awaitTermination()
    return [(p.batchId, p.numInputRows) for p in q.recentProgress if p.numInputRows]


print("run 1, batches (id, rows):", run())
for f in ("commits/2", "commits/.2.crc"):
    (w / "ck" / f).unlink(missing_ok=True)
print("removed commits/2: offsets", sorted(int(p.name) for p in (w / "ck/offsets").iterdir() if p.name.isdigit()),
      "commits", sorted(int(p.name) for p in (w / "ck/commits").iterdir() if p.name.isdigit()))
print("run 2, batches (id, rows):", run())

got = (spark.read.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "rides_out")
       .option("includeHeaders", "true").load()
       .withColumn("src", F.expr("cast(filter(headers, h -> h.key = 'src')[0].value as string)")))
print("\nrides_out holds:")
print(f"  records            {got.count()}")
print(f"  distinct keys      {got.select('key').distinct().count()}   (trip_id)")
print(f"  distinct src       {got.select('src').distinct().count()}   (input topic:partition:offset)")
dups = got.groupBy("key").count().where("count > 1")
print(f"  keys written twice {dups.count()}")

print("\nA reader that keeps one record per key gets the input exactly:")
latest = got.dropDuplicates(["key"])
print(f"  dropDuplicates(['key']) -> {latest.count()} records, {n_in} trips published")

print("\nThe sink accepts no Kafka transactions:")
try:
    (spark.range(3).selectExpr("cast(id as string) as value").write.format("kafka")
     .option("kafka.bootstrap.servers", PLAIN).option("topic", "rides_out")
     .option("kafka.transactional.id", "spark-tx-1").save())
    print("  kafka.transactional.id accepted")
except Exception as e:
    print(err(e, 86, 3))
spark.stop()
