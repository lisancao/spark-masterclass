"""Step 3: dead letters and silver through foreachBatch, safe when Spark replays a batch.
Then housekeeping: cleanSource=archive and its two surprises.

Run from code/:  uv run python ch25/m3_dead_letters.py
"""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, error_text
from clicks import drop, parse_events

spark = session("ch25-dead-letters", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC")
w = work("ch25/m3")
inbox, lake, ckpt = w / "inbox", w / "lake", w / "ck_ingest"
inbox.mkdir()


def route_batch(batch, batch_id):
    """Write one micro-batch so that running it again changes nothing."""
    batch = batch.persist()                   # used twice: read and parse the files once
    try:
        (batch.where("error IS NOT NULL").select("line", "error")
         .write.mode("overwrite").json(str(lake / f"dead_letter/batch_id={batch_id}")))
        good = (batch.where("error IS NULL")
                .select("event_id", "user_id", "type", "page", "ts", "unknown_keys")
                .dropDuplicates(["event_id"]))                       # duplicates inside the batch
        silver = lake / "silver"
        if silver.exists():                                          # ... and in earlier batches
            seen = (spark.read.parquet(str(silver))
                    .where(F.col("batch_id") != batch_id).select("event_id"))
            good = good.join(seen, "event_id", "left_anti")
        good.write.mode("overwrite").parquet(str(silver / f"batch_id={batch_id}"))
    finally:
        batch.unpersist()


def ingest():
    q = (parse_events(spark.readStream.text(str(inbox)))
         .writeStream.foreachBatch(route_batch)
         .option("checkpointLocation", str(ckpt))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    return q


def counts():
    s = spark.read.parquet(str(lake / "silver"))
    d = spark.read.json(str(lake / "dead_letter"))
    return s.count(), s.select("event_id").distinct().count(), d.count()


for i in range(1, 5):
    drop(inbox, i)
    q = ingest()
    p = q.recentProgress[-1]
    s, distinct, d = counts()
    print(f"drop {i}: batch {p.batchId}, {p.numInputRows:>2} lines -> silver {s} ({distinct} distinct ids), dead letters {d}")

print("\nDead letters, with the raw line kept for replay:")
spark.read.json(str(lake / "dead_letter")).select("batch_id", "error", F.substring("line", 1, 50).alias("line")) \
    .orderBy("batch_id").show(truncate=False)

last = max(int(f.name) for f in (ckpt / "commits").iterdir() if f.name.isdigit())
for name in (str(last), f".{last}.crc"):
    (ckpt / "commits" / name).unlink()
print(f"Deleted commits/{last}: the checkpoint now says batch {last} was planned, not finished.")
q = ingest()
print("restart ran batches", [p.batchId for p in q.recentProgress if p.numInputRows],
      "-> silver %d (%d distinct), dead letters %d" % counts())

print("\nHousekeeping: cleanSource=archive")
w2 = work("ch25/m3-archive")
box, arch = w2 / "inbox", w2 / "archive"
box.mkdir()


def archive_run(**opts):
    q = (spark.readStream.options(**opts).text(str(box)).writeStream.format("noop")
         .option("checkpointLocation", str(w2 / "ck")).trigger(availableNow=True).start())
    q.awaitTermination()


rel = lambda root: sorted(str(p.relative_to(root)) for p in root.rglob("*.jsonl")) if root.exists() else []
drop(box, 1)
drop(box, 2)
archive_run(maxFilesPerTrigger=1, cleanSource="archive", sourceArchiveDir=str(arch))
print("  after run 1   inbox:", rel(box), " archive:", [r.split("inbox/")[-1] for r in rel(arch)])
print("  archived as:   archive/" + rel(arch)[0].replace(str(Path.home()).lstrip("/"), "~"))
drop(box, 3)
archive_run(maxFilesPerTrigger=1, cleanSource="archive", sourceArchiveDir=str(arch))
print("  after run 2   inbox:", rel(box), " archive:", [r.split("inbox/")[-1] for r in rel(arch)])
for opts in ({"cleanSource": "archive"}, {"cleanSource": "archive", "sourceArchiveDir": str(box / "old")}):
    try:
        archive_run(**opts)
    except Exception as e:
        print(f"  {list(opts)}:\n" + error_text(e, lines=3).replace(str(Path.home()), "~"))
