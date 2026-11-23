"""Chapter 31: files as files. Listing, globs, modification-time filters and _metadata.

Run from code/:  uv run python ch31/listing.py
"""
import calendar
import os
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, show, MESSY, WORK

spark = session("ch31-listing", spark_sql_session_timeZone="UTC")
rel = F.regexp_replace("path", r"^.*/messy/", "")      # print paths relative to the data folder


def section(title):
    print(f"\n== {title} ==")


section("one row per file")
audio = spark.read.format("binaryFile").load(str(MESSY / "audio"))
audio.printSchema()
show(audio.select(rel.alias("path"), "length", F.hex(F.substring("content", 1, 12)).alias("first 12 bytes")))

section("a folder that holds only folders")
images = str(MESSY / "images")
print("load(images).count()                         ->",
      spark.read.format("binaryFile").load(images).count())
rec = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(images)
print("recursiveFileLookup=true                     ->", rec.count())
pngs = (spark.read.format("binaryFile").option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.png").load(images))
print("  + pathGlobFilter=*.png                     ->", pngs.count())
print("load(images/*/*.png)                         ->",
      spark.read.format("binaryFile").load(images + "/*/*.png").count())
print("load([pdf, mail])                            ->",
      spark.read.format("binaryFile").load([str(MESSY / "pdf"), str(MESSY / "mail")]).count())
print("pathGlobFilter matching nothing              ->",
      spark.read.format("binaryFile").option("pathGlobFilter", "*.gif").load(images + "/*").count())
try:
    spark.read.format("binaryFile").load(images + "/*/*.gif").count()
except Exception as e:
    print("a path glob matching nothing                 ->", str(e).split("]")[0] + "]")

section("partition folders: discovered, or not")
days = WORK / "ch31" / "landing"
shutil.rmtree(days, ignore_errors=True)
for d in ("2026-10-01", "2026-10-02"):
    (days / f"day={d}").mkdir(parents=True)
    shutil.copy(MESSY / "events" / f"events_{d}.json", days / f"day={d}" / "events.json")
(days / "day=2026-10-02" / "_SUCCESS").touch()
(days / "day=2026-10-02" / ".events.json.crc").write_text("x")
by_dir = spark.read.format("binaryFile").load(str(days))
print("columns:", by_dir.columns)
show(by_dir.select(F.regexp_replace("path", r"^.*/landing/", "").alias("path"), "day"))
flat = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(str(days))
print("with recursiveFileLookup, columns:", flat.columns)

section("modifiedAfter: only what changed since the last run")
uploads = WORK / "ch31" / "uploads"
shutil.rmtree(uploads, ignore_errors=True)
uploads.mkdir(parents=True)
shutil.copy(MESSY / "images/products/red_16x8.png", uploads / "upload_0001.png")
shutil.copy(MESSY / "audio/tone_440.wav", uploads / "voice_note.wav")
old = calendar.timegm((2020, 1, 1, 0, 0, 0))     # 2020-01-01 00:00 UTC
os.utime(uploads / "voice_note.wav", (old, old))
show(spark.read.format("binaryFile").load(str(uploads))
     .select(F.regexp_replace("path", r"^.*/uploads/", "").alias("file"), "modificationTime"))
recent = (spark.read.format("binaryFile").option("modifiedAfter", "2025-01-01T00:00:00")
          .load(str(uploads)))
print("modifiedAfter=2025-01-01T00:00:00 ->",
      [r.path.rsplit("/", 1)[1] for r in recent.select("path").collect()])
for bad in ("01/01/2025", "2025-01-01"):
    try:
        n = (spark.read.format("binaryFile").option("modifiedAfter", bad)
             .load(str(uploads)).count())
        print(f"modifiedAfter={bad!r:14} -> {n} file(s)")
    except Exception as e:
        print(f"modifiedAfter={bad!r:14} ->", str(e).strip().splitlines()[0][:110])

section("the hidden _metadata column")
events = spark.read.json(str(MESSY / "events"))
print("'_metadata' in events.columns:", "_metadata" in events.columns)
events.select("_metadata").printSchema()
show(events.groupBy(F.col("_metadata.file_name").alias("file_name"),
                    F.col("_metadata.file_size").alias("file_size"))
     .count().orderBy("file_name"))
trips_md = spark.read.parquet(str(Path(os.environ.get("BOOK_DATA", "")) / "nyc-taxi/yellow")) \
    if os.environ.get("BOOK_DATA") else None
if trips_md is not None:
    trips_md.select("_metadata").printSchema()
    show(trips_md.select("_metadata.row_index", "_metadata.file_block_start",
                         "_metadata.file_block_length")
         .where(F.col("_metadata.row_index").isin(0, 1048575, 1048576)).orderBy("row_index"))
