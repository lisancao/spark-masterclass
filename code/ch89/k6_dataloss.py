"""Step 6: failOnDataLoss. Records that the query did not read yet leave the topic.

A query reads the topic `ledger` and stops. More records arrive. Then the broker deletes
records past the query's checkpoint (as retention does), and the query starts again.
Run from code/:  uv run python ch89/k6_dataloss.py   (after ch89/kafka.sh start)
"""
import json
import subprocess

from k89 import spark_session, work, kafka, publish, slices, trip_rows, end_offsets, err as error_text, j, PLAIN

spark = spark_session("ch89-dataloss")
w = work("ch89/k6")
kafka("topic", "ledger", "2")
lines = trip_rows(slices()[:6])


def run(fail_on_loss):
    q = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "ledger")
         .option("startingOffsets", "earliest").option("failOnDataLoss", fail_on_loss).load()
         .writeStream.format("noop").option("checkpointLocation", str(w / "ck")).trigger(availableNow=True).start())
    try:
        q.awaitTermination()
    except Exception as e:
        return error_text(e, 86, 5)
    return f"batches (id, rows) {[(p.batchId, p.numInputRows) for p in q.recentProgress if p.numInputRows]}"


def checkpoint_offsets():
    ck = w / "ck/offsets"
    last = max(int(f.name) for f in ck.iterdir() if f.name.isdigit())
    return json.loads((ck / str(last)).read_text().splitlines()[2])["ledger"]


publish(spark, "ledger", lines[:1000])
print("1. 1000 records published. Run 1:", run("true"))
seen = checkpoint_offsets()
print("   the checkpoint says: next offsets", seen)

publish(spark, "ledger", lines[1000:2000])
ends = end_offsets("ledger")
print(f"\n2. 1000 more records published. End offsets now {ends}")
cut = {p: seen[str(p)] + 200 for p in ends}                     # delete 200 unread records per partition
spec = {"partitions": [{"topic": "ledger", "partition": p, "offset": o} for p, o in cut.items()], "version": 1}
subprocess.run(["docker", "exec", "-i", "sm-v3stream-kafka", "sh", "-c", "cat > /tmp/del.json"],
               input=json.dumps(spec), text=True, check=True)
out = kafka("cli", "kafka-delete-records", "--offset-json-file", "/tmp/del.json")
print("   kafka-delete-records:", " ".join(l.strip() for l in out.splitlines() if "low_watermark" in l))
lost = sum(cut[p] - seen[str(p)] for p in cut)
print(f"   {lost} records that the query did not read are gone")

print("\n3. Run 2, failOnDataLoss=true (the default):")
print(run("true"))
print("\n4. Run 3, the same checkpoint, failOnDataLoss=false:")
print("  ", run("false"))
print("   records read in run 3 + records deleted =", sum(ends[p] - cut[p] for p in ends), "+", lost,
      "=", sum(ends[p] - seen[str(p)] for p in ends), "(the records after run 1)")

print("\n5. A batch read that starts before the first record that is left:")
for f in ("true", "false"):
    df = (spark.read.format("kafka").option("kafka.bootstrap.servers", PLAIN).option("subscribe", "ledger")
          .option("startingOffsets", j({"ledger": {"0": 0, "1": 0}})).option("failOnDataLoss", f).load())
    try:
        print(f"   failOnDataLoss={f}: {df.count()} rows")
    except Exception as e:
        print(f"   failOnDataLoss={f}:" + error_text(e, 80, 2).strip())
spark.stop()
