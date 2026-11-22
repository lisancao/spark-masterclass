"""Step 11: change a query and keep its checkpoint. Named sources (experimental, behind an
internal flag), and the 4.2+ check for a checkpoint that lost its metadata file.

Run from code/:  uv run python ch25/s11_evolve.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, deliver, error_text
from replay import slices, SCHEMA

spark = session("ch25-evolve", spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC")
w = work("ch25/s11")
yellow, green = w / "yellow", w / "green"
yellow.mkdir()
green.mkdir()
files = slices()


def run(df, ck):
    q = (df.groupBy("PULocationID").count().writeStream.format("noop").outputMode("update")
         .option("checkpointLocation", str(ck)).trigger(availableNow=True).start())
    q.awaitTermination()
    return [(p.batchId, p.numInputRows) for p in q.recentProgress]


def src(path, name=None):
    r = spark.readStream.schema(SCHEMA)
    if name:
        r = r.name(name)
    return r.json(str(path))


print("1. Without the flag, .name() is refused:")
try:
    src(yellow, "yellow_trips")
except Exception as e:
    print(error_text(e, lines=2))

print("\n2. Positional sources: add a second source on restart")
deliver(yellow, *files[0][:2])
print("   run 1, yellow only:   ", run(src(yellow), w / "ck_pos"))
deliver(green, *files[1][:2])
try:
    print("   run 2, yellow + green:", run(src(yellow).unionByName(src(green)), w / "ck_pos"))
except Exception as e:
    print(error_text(e, lines=3))

spark.conf.set("spark.sql.streaming.queryEvolution.enableSourceEvolution", "true")
print("\n3. With enableSourceEvolution=true and a name on every source")
print("   run 1, yellow only:   ", run(src(yellow, "yellow_trips"), w / "ck_named"))
deliver(yellow, *files[2][:2])
print("   run 2, yellow + green:",
      run(src(yellow, "yellow_trips").unionByName(src(green, "green_trips")), w / "ck_named"))
print("   offsets/1, the source lines:")
for line in (w / "ck_named/offsets/1").read_text().splitlines()[2:]:
    print("    ", line)
try:
    run(src(yellow, "yellow_trips").unionByName(src(green)), w / "ck_named")
except Exception as e:
    print("   one source left unnamed:\n" + error_text(e, lines=2))
spark.conf.set("spark.sql.streaming.queryEvolution.enableSourceEvolution", "false")

print("\n4. Remove the metadata file of a checkpoint that has offsets and commits:")
(w / "ck_pos/metadata").unlink()
try:
    run(src(yellow), w / "ck_pos")
except Exception as e:
    print(error_text(e, lines=3))
