"""Step 3: what a stateful Real-time query looks like while it runs: its physical plan, and its
stages in the Spark UI's REST API, both active at the same time (the pipelined shuffle).

Start the broker first:  ch24/kafka.sh start
Run from code/:          uv run python ch27/r4_pipeline.py
"""
import contextlib
import io
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, ui_api, KAFKA_PACKAGE, KAFKA_BOOTSTRAP
from producer import produce

spark = session("ch27-pipeline", spark_sql_shuffle_partitions="3", spark_jars_packages=KAFKA_PACKAGE)
w = work("ch27/r4")
KAFKA = Path(__file__).resolve().parents[1] / "ch24" / "kafka.sh"
subprocess.run([str(KAFKA), "topic", "pipe_in", "4"], check=True, capture_output=True)

counts = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
          .option("subscribe", "pipe_in").load()
          .groupBy(F.col("key").cast("string").alias("user")).count())


def stages(label):
    print(f"{label}: stages in the REST API (status, tasks: active/complete)")
    for s in sorted(ui_api("stages"), key=lambda s: s["stageId"])[-4:]:
        print(f"  stage {s['stageId']:>3}  {s['status']:<9} tasks {s['numActiveTasks']}/{s['numCompleteTasks']}"
              f" of {s['numTasks']}  {s['name'][:40]}")


threading.Thread(target=produce, args=("pipe_in", 50, 25), daemon=True).start()
q = (counts.writeStream.format("console").outputMode("update")
     .option("checkpointLocation", str(w / "rtm")).trigger(realTime="30 seconds").start())
time.sleep(10)
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    q.explain()
print("Real-time Mode, the physical plan (expression ids removed):")
for line in buf.getvalue().splitlines():
    if line.strip() and not line.startswith("=="):
        print("  " + re.sub(r"#\d+L?", "", line)[:96])
stages("Real-time Mode, 10 s into a 30 s batch")
q.stop()

q = (counts.writeStream.format("noop").outputMode("update")
     .option("checkpointLocation", str(w / "mb")).trigger(processingTime="1 second").start())
time.sleep(8)
q.stop()
stages("\nMicro-batch, after a few batches")
