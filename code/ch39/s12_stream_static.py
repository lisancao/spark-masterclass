"""Step 12: stream-static joins in depth. Which join types a stream accepts with a static side,
how Spark plans the join, and when a change to the static side reaches the stream.

Run from code/:  uv run python ch39/s12_stream_static.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver, error_text, plan, ZONES
from replay import slices, SCHEMA

spark = session("ch39-stream-static", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC",
                spark_ui_showConsoleProgress="false")
w = work("ch39/s12")
(w / "inbox").mkdir()
files = slices()
trips = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "inbox"))
zones = spark.read.option("header", True).csv(str(ZONES)).select(
    F.col("LocationID").cast("int").alias("PULocationID"), "Borough", "Zone")

print("1. Join types between a stream (left) and a static table (right), and the reverse:")
cases = [("stream INNER static", lambda: trips.join(zones, "PULocationID", "inner")),
         ("stream LEFT OUTER static", lambda: trips.join(zones, "PULocationID", "left")),
         ("stream LEFT SEMI static", lambda: trips.join(zones, "PULocationID", "left_semi")),
         ("stream LEFT ANTI static", lambda: trips.join(zones, "PULocationID", "left_anti")),
         ("stream RIGHT OUTER static", lambda: trips.join(zones, "PULocationID", "right")),
         ("stream FULL OUTER static", lambda: trips.join(zones, "PULocationID", "full")),
         ("static LEFT OUTER stream", lambda: zones.join(trips, "PULocationID", "left")),
         ("static RIGHT OUTER stream", lambda: zones.join(trips, "PULocationID", "right"))]
for label, make in cases:
    try:
        q = make().writeStream.format("noop").option("checkpointLocation", str(w / "ck" / label.replace(" ", "_"))).start()
        q.stop()
        print(f"   {label:<27} ok")
    except Exception as e:
        print(f"   {label:<27}refused:\n" + error_text(e, 86, 2))

print("\n2. The physical plan of one micro-batch (stream LEFT OUTER static):")
deliver(w / "inbox", files[0][0], files[0][1])
q = (trips.join(zones, "PULocationID", "left").groupBy("Borough").count()
     .writeStream.format("memory").queryName("j").outputMode("complete")
     .option("checkpointLocation", str(w / "ck_plan")).trigger(availableNow=True).start())
q.awaitTermination()
import contextlib, io
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    q.explain()                                   # the physical plan of the last micro-batch
txt = buf.getvalue()
for line in txt.splitlines():
    if any(k in line for k in ("Join", "Broadcast", "FileScan", "Exchange", "StateStore", "Scan csv", "Scan json")):
        print("  ", line.replace(str(Path.home()), "~")[:118])

print("\n3. When does a change to the static side reach the stream?")
(w / "inbox2").mkdir()
labels = w / "labels"
LABEL = "PULocationID INT, label STRING"


def write_labels(name, rows, mode="errorifexists"):
    spark.createDataFrame(rows, LABEL).coalesce(1).write.mode(mode).parquet(str(labels / name))


def read_labels():
    return spark.read.schema(LABEL).option("recursiveFileLookup", True).parquet(str(labels))


write_labels("v1", [(132, "airport"), (138, "airport")])
static = read_labels()                                  # a static DataFrame over a folder of Parquet files
seen = {}


def run(stream_df, body, name, steps):
    q = (stream_df.writeStream.foreachBatch(body).option("checkpointLocation", str(w / name))
         .trigger(processingTime="1 second").start())
    try:
        for k, note, change in steps:
            if change:
                change()
            deliver(w / "inbox2", files[k][0], files[k][1])
            q.processAllAvailable()
            print(f"   batch {max(seen)}: labels found {seen[max(seen)]}   ({note})")
    except Exception as e:
        print("   " + error_text(e, 86, 3).strip())
    finally:
        q.stop()


def count_labels(df):
    return dict(sorted((r.label or "-", r["count"]) for r in df.groupBy("label").count().collect()))


steps = [(1, "v1 has two airport zones", None),
         (2, "a new file v2 added", lambda: write_labels("v2", [(161, "midtown"), (237, "uptown")])),
         (3, "v1 rewritten with a third zone", lambda: write_labels("v1", [(132, "airport"), (138, "airport"), (230, "times-sq")], "overwrite"))]

print("   a) join with the static DataFrame in the query:")
stream2 = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "inbox2"))
run(stream2.join(static, "PULocationID", "left"),
    lambda df, b: seen.__setitem__(b, count_labels(df)), "ck_a", steps)

print("   b) read the static side again in each batch, inside foreachBatch:")
import shutil
shutil.rmtree(labels)
shutil.rmtree(w / "inbox2")
(w / "inbox2").mkdir()
write_labels("v1", [(132, "airport"), (138, "airport")])
seen.clear()


def fresh_join(df, batch_id):
    seen[batch_id] = count_labels(df.join(read_labels(), "PULocationID", "left"))


run(spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "inbox2")), fresh_join, "ck_b", steps)
spark.stop()
