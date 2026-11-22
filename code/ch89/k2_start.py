"""Step 2: where a new streaming query starts in a topic.

Three deliveries go to the topic `trips` (4 partitions), two seconds apart. Then new queries
start with each starting option, each on a new checkpoint, with availableNow.
Run from code/:  uv run python ch89/k2_start.py   (after ch89/kafka.sh start)
"""
import time

from pyspark.sql import functions as F
from k89 import spark_session, work, kafka, publish, slices, trip_rows, end_offsets, now_ms, error_text, j, PLAIN

spark = spark_session("ch89-start")
w = work("ch89/k2")
kafka("topic", "trips", "4")
files = slices()
marks = []
for i, chunk in enumerate([files[0:2], files[2:4], files[4:6]]):
    if i:
        time.sleep(2)
    marks.append(now_ms())
    n = publish(spark, "trips", trip_rows(chunk))
    print(f"delivery {i + 1}: {n} trips, sent after t={marks[-1]}")
ends = end_offsets("trips")
print("end offsets per partition (kafka-get-offsets):", ends, "total", sum(ends.values()))


def count(name, **opts):
    """Start a new query (new checkpoint) with these options, availableNow, and count rows per partition."""
    r = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "trips"))
    for k, v in opts.items():
        r = r.option(k, v)
    q = (r.load().groupBy("partition").agg(F.count("*").alias("n"))
         .writeStream.format("memory").queryName(name).outputMode("complete")
         .option("checkpointLocation", str(w / name)).trigger(availableNow=True).start())
    try:
        q.awaitTermination()
    except Exception as e:
        return error_text(e, 86, 3)
    got = {r.partition: r.n for r in spark.table(name).collect()}
    return f"{sum(got.values()):>5} rows  " + " ".join(f"p{p}={got.get(p, 0)}" for p in range(4))


t2, t3 = marks[1], marks[2]
cases = [
    ("default (latest)", {}),
    ("earliest", {"startingOffsets": "earliest"}),
    ("JSON offsets", {"startingOffsets": j({"trips": {"0": 100, "1": -2, "2": -1, "3": 300}})}),
    ("JSON, p3 past its end", {"startingOffsets": j({"trips": {"0": 100, "1": -2, "2": -1, "3": 500}})}),
    ("startingTimestamp=t2", {"startingTimestamp": str(t2)}),
    ("startingOffsetsByTimestamp", {"startingOffsetsByTimestamp": j({"trips": {"0": t3, "1": t2, "2": t2, "3": t3}})}),
    ("timestamp in the future", {"startingTimestamp": str(now_ms() + 3_600_000)}),
    ("future, strategy latest", {"startingTimestamp": str(now_ms() + 3_600_000),
                                  "startingOffsetsByTimestampStrategy": "latest"}),
]
print("\nA new query on each line (availableNow, new checkpoint):")
for i, (label, opts) in enumerate(cases):
    print(f"  {label:<28}{count(f'c{i}', **opts)}")
print("  (JSON: p0 from offset 100, p1 earliest (-2), p2 latest (-1), p3 from offset 300, then 500)")

print("\nThe rule for partitions that appear later: they start at earliest.")
ck = w / "grow"


def grow_run():
    q = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "trips")
         .option("startingOffsets", "latest").load()
         .writeStream.format("noop").option("checkpointLocation", str(ck)).trigger(availableNow=True).start())
    q.awaitTermination()
    return [(p.batchId, p.numInputRows) for p in q.recentProgress if p.numInputRows]


print("  run 1, startingOffsets=latest: batches with rows", grow_run())
kafka("cli", "kafka-topics", "--alter", "--topic", "trips", "--partitions", "6")
# A new client.id gives a new producer: the pooled producer still has the metadata of 4 partitions.
n = publish(spark, "trips", trip_rows(files[6:8]), opts={"kafka.client.id": "after-alter"})
after = end_offsets("trips")
print(f"  topic altered to 6 partitions, then {n} trips published; end offsets {after}")
print("  run 2, same checkpoint:      batches with rows", grow_run())
last = max(int(f.name) for f in (ck / "offsets").iterdir() if f.name.isdigit())
print(f"  offsets/{last} source line:", (ck / f"offsets/{last}").read_text().splitlines()[2])
spark.stop()
