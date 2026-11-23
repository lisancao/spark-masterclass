"""Step 6: the same Real-time query into a Python foreach sink. Latency measured inside the sink:
the time process() sees a row minus the row's Kafka CreateTime. Run once with the default
spark.buffer.size and once with a small buffer.

Start the broker first:  ch48/kafka.sh start
Run from code/:          uv run python ch51/r3_foreach.py [buffer_size]
"""
import json
import subprocess
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, KAFKA_PACKAGE, KAFKA_BOOTSTRAP
from producer import produce

BUFFER = sys.argv[1] if len(sys.argv) > 1 else None
conf = {"spark_buffer_size": BUFFER} if BUFFER else {}
spark = session("ch51-foreach", spark_jars_packages=KAFKA_PACKAGE, **conf)
w = work(f"ch51/r3_{BUFFER or 'default'}")
log = w / "sink.jsonl"
KAFKA = Path(__file__).resolve().parents[1] / "ch48" / "kafka.sh"
subprocess.run([str(KAFKA), "topic", "fe_in", "1"], check=True, capture_output=True)


class Sink:
    """A Python ForeachWriter: one line per row, with the time that Python saw it."""

    def open(self, partition_id, epoch_id):
        self.f = open(log, "a")
        return True

    def process(self, row):
        self.f.write(json.dumps({"v": row.v, "in_ms": row.in_ms, "seen_ms": int(time.time() * 1000)}) + "\n")
        self.f.flush()

    def close(self, error):
        self.f.close()


q = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
     .option("subscribe", "fe_in").option("startingOffsets", "latest").load()
     .select(F.col("value").cast("string").alias("v"), F.unix_millis("timestamp").alias("in_ms"))
     .writeStream.foreach(Sink()).option("checkpointLocation", str(w / "ck"))
     .outputMode("update").trigger(realTime="20 seconds").start())
time.sleep(8)
produce("fe_in", rate=1, seconds=10)             # one record a second
time.sleep(25)
q.stop()
rows = [json.loads(l) for l in log.read_text().splitlines()]
print(f"spark.buffer.size = {BUFFER or 'default (65536)'}: one record a second, 20 s Real-time batches")
for r in sorted(rows, key=lambda r: int(r["v"])):
    print(f"  record {r['v']:>2}  reached Python after {r['seen_ms'] - r['in_ms']:>6} ms")
