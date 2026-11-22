"""Step 8: consumer groups. What Kafka sees of a Spark query, and how to see the lag.

A query reads `trips4` slowly (maxOffsetsPerTrigger) while the script asks the broker about
consumer groups. Then a second query sets groupIdPrefix, and a third sets kafka.group.id.
Run from code/:  uv run python ch89/k8_groups.py   (after ch89/k4_admission.py made trips4)
"""
import time

from k89 import spark_session, work, kafka, PLAIN

spark = spark_session("ch89-groups")
w = work("ch89/k8")


def start(name, **opts):
    r = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "trips4")
         .option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 200))
    for k, v in opts.items():
        r = r.option(k, v)
    return (r.load().writeStream.queryName(name).format("noop").option("checkpointLocation", str(w / name))
            .trigger(processingTime="2 seconds").start())


def groups():
    return sorted(g for g in kafka("cli", "kafka-consumer-groups", "--list").split() if g.startswith(("spark", "my", "shared")))


before = set(groups())
q = start("plain")
time.sleep(9)
new = [g for g in groups() if g not in before]
print("1. Consumer groups that appeared while the query ran:")
for g in new:
    print("   ", g)
print("\n2. kafka-consumer-groups --describe for that group:")
for g in new:
    out = kafka("cli", "kafka-consumer-groups", "--describe", "--group", g, check=False)
    for line in out.strip().splitlines():
        print("   ", " ".join(line.split()))

print("\n3. The lag, from Spark's own progress report (source metrics):")
p = q.lastProgress
s = p["sources"][0]
print("    batch", p["batchId"], "endOffset", s["endOffset"])
print("    latestOffset", s["latestOffset"])
print("    metrics", s["metrics"])
q.stop()

before = set(groups())
q2 = start("prefixed", groupIdPrefix="myteam-orders")
q3 = start("forced", **{"kafka.group.id": "shared-reader"})
time.sleep(7)
print("\n4. groupIdPrefix and kafka.group.id:")
for g in groups():
    if g not in before or g == "shared-reader":
        print("   ", g)
print("    progress of the query with kafka.group.id:", [(x.batchId, x.numInputRows) for x in q3.recentProgress][:4])
q2.stop()
q3.stop()
spark.stop()
