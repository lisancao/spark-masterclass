"""Step 14: foreachBatch patterns. One batch to two sinks, a batch log that makes a replay
harmless, a failure inside the function, and the row-level foreach writer.

Run from code/:  uv run python ch39/s14_foreach_batch.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver, error_text, ZONES
from replay import slices, SCHEMA

spark = session("ch39-foreach-batch", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC",
                spark_ui_showConsoleProgress="false")
w = work("ch39/s14")
(w / "inbox").mkdir()
files = slices()
for k in range(3):
    deliver(w / "inbox", files[k][0], files[k][1], mtime=1_000_000 + k)
zones = spark.read.option("header", True).csv(str(ZONES)).select(
    F.col("LocationID").cast("int").alias("PULocationID"), "Borough")
trips = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "inbox"))
calls = []


def fan_out(df, batch_id):
    """Pattern 1 and 2: two sinks from one batch, and a batch log that skips a batch already written."""
    done = w / "out/_batches" / str(batch_id)
    if done.exists():                                    # this batch_id was written before: a replay
        calls.append((batch_id, "skipped (in the batch log)"))
        return
    df = df.persist()
    (df.join(zones, "PULocationID").groupBy("Borough").agg(F.count("*").alias("trips"))
       .write.mode("overwrite").parquet(str(w / f"out/by_borough/batch_id={batch_id}")))
    (df.where("trip_distance > 15").select("trip_id", "trip_distance", "total_amount")
       .write.mode("overwrite").json(str(w / f"out/long_trips/batch_id={batch_id}")))
    df.unpersist()
    done.parent.mkdir(parents=True, exist_ok=True)
    done.touch()                                         # the last action: mark the batch as written
    calls.append((batch_id, "written"))


def run(fn, name):
    q = (trips.writeStream.foreachBatch(fn).option("checkpointLocation", str(w / name))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    return q


print("1. One batch, two sinks, and a batch log:")
run(fan_out, "ck")
print("   run 1 calls:", calls)
for f in ("commits/2", "commits/.2.crc"):
    (w / "ck" / f).unlink(missing_ok=True)                # as if the driver died before commits/2
calls.clear()
run(fan_out, "ck")
print("   commits/2 removed, run 2 calls:", calls)
print("   files on disk:")
for p in sorted((w / "out").glob("*/batch_id=*")):
    print("    ", p.relative_to(w / "out"), len(list(p.glob("part-*"))), "part file(s)")
print("    ", "_batches:", sorted(p.name for p in (w / "out/_batches").iterdir()))

print("\n2. An exception inside the function fails the query. The retry gets the same batch_id:")
seen = []


def fragile(df, batch_id):
    seen.append(batch_id)
    if batch_id == 1 and seen.count(1) == 1:
        raise RuntimeError("the warehouse is down")


for attempt in (1, 2):
    try:
        run(fragile, "ck_fragile")
        print(f"   attempt {attempt}: completed")
    except Exception as e:
        print(f"   attempt {attempt}:" + error_text(e, 80, 1).strip()[:96])
print("   batch_id values the function saw:", seen)

print("\n3. foreach: a writer object, called for each row in each partition:")


LOGS = str(w / "foreach_logs")
Path(LOGS).mkdir()


class RowWriter:
    def open(self, partition_id, epoch_id):
        self.n = 0
        self.key = (epoch_id, partition_id)
        return True                                      # False skips this partition

    def process(self, row):
        self.n += 1

    def close(self, error):                              # runs in the Python worker: write a small log file
        log = Path(LOGS) / f"epoch{self.key[0]}-partition{self.key[1]}.txt"
        log.write_text(f"{self.n} rows, error={error}")


q = (trips.repartition(2).writeStream.foreach(RowWriter()).option("checkpointLocation", str(w / "ck_rows"))
     .trigger(availableNow=True).start())
q.awaitTermination()
for f in sorted(Path(LOGS).iterdir()):
    print(f"     {f.stem}: {f.read_text()}")
spark.stop()
