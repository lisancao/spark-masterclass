"""Chapter 11, new in Spark 4.3: read the files inside zip and tar archives.

Run from code/:  uv run python ch11/archives.py
"""
import gzip
import shutil
import sys
import tarfile
import zipfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, MESSY, WORK

spark = session("ch11-archives", spark_sql_session_timeZone="UTC")
drop = WORK / "ch11" / "drop"
shutil.rmtree(drop, ignore_errors=True)
drop.mkdir(parents=True)
short = F.regexp_replace("path", r"^.*/drop/", "")


def section(title):
    print(f"\n== {title} ==")


# A partner's export: three days of events in a zip, with a README and the folder macOS adds.
with zipfile.ZipFile(drop / "events.zip", "w", zipfile.ZIP_DEFLATED) as z:
    for f in sorted((MESSY / "events").glob("*.json")):
        z.write(f, f"export/{f.name}")
    z.writestr("export/README.txt", "Exported by the partner portal.\n")
    z.writestr("__MACOSX/export/._events_2026-10-01.json", b"\x00\x05\x16\x07")
with tarfile.open(drop / "logs.tar.gz", "w:gz") as t:
    for name in ("access.log", "app.log"):
        t.add(MESSY / "logs" / name, f"logs/{name}")
with gzip.open(drop / "events_2026-10-01.json.gz", "wb") as g:
    g.write((MESSY / "events" / "events_2026-10-01.json").read_bytes())

section("the drop")
for f in sorted(drop.iterdir()):
    print(f"  {f.name:28} {f.stat().st_size:6} bytes")
for f in ("events.zip",):
    with zipfile.ZipFile(drop / f) as z:
        for i in z.infolist():
            print(f"    {f}!/{i.filename:42} {i.file_size:5} -> {i.compress_size:5} bytes")

section("before 4.3 (and with the flag off): an archive is just bytes")
gz = spark.read.json(str(drop / "events_2026-10-01.json.gz"))
print("json, .json.gz (a codec, not an archive):", gz.count(), "rows")
z = spark.read.json(str(drop / "events.zip"))
print("json, .zip:", z.columns, z.count(), "rows")

section("spark.sql.files.archive.reader.enabled=true")
spark.conf.set("spark.sql.files.archive.reader.enabled", "true")
events = spark.read.json(str(drop / "events.zip")).cache()
print("columns:", events.columns)
print("rows:", events.count())
show(events.where(F.col("_corrupt_record").isNotNull()).select("_corrupt_record"))

section("archivePathFilter picks the entries")
only_json = spark.read.option("archivePathFilter", "*.json").json(str(drop / "events.zip"))
print("archivePathFilter=*.json ->", only_json.count(), "rows,",
      "_corrupt_record" in only_json.columns and
      only_json.cache().where(F.col("_corrupt_record").isNotNull()).count(), "corrupt")
show(only_json.groupBy(F.col("_metadata.file_name").alias("_metadata.file_name"),
                       F.col("_metadata.file_size").alias("_metadata.file_size")).count())
print("scan tasks for one archive:", only_json.rdd.getNumPartitions())

section("text from a tar.gz")
logs = spark.read.text(str(drop / "logs.tar.gz"))
print("lines:", logs.count(), " first:", logs.first().value[:70])

section("binaryFile: one row per archive, or one per entry with wholeFile=false")
show(spark.read.format("binaryFile").load(str(drop)).select(short.alias("path"), "length"))
entries = spark.read.format("binaryFile").option("wholeFile", "false").load(str(drop))
show(entries.select(short.alias("path"), "length",
                    F.hex(F.substring("content", 1, 4)).alias("first 4 bytes")))
