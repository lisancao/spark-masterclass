"""Step 7: which changes to a stateful query survive a restart on the same checkpoint.

The base query counts clean events per user in 10-minute windows. Each experiment starts
from a fresh copy of the checkpoint after drops 1 and 2, changes one thing, and runs drop 3.
Run from code/:  uv run python ch40/m6_compat.py
"""
import re
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, ROCKSDB
from clicks import drop, parse_events

spark = session("ch40-compat", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC")
w = work("ch40/m6")
inbox, base = w / "inbox", w / "ck_base"
inbox.mkdir()


def query(delay="10 minutes", keys=("user_id",), aggs=None, where="error IS NULL", dedup=False):
    ev = parse_events(spark.readStream.text(str(inbox))).where(where).withWatermark("ts", delay)
    if dedup:
        ev = ev.dropDuplicatesWithinWatermark(["event_id"])
    aggs = aggs or [F.count("*").alias("events")]
    return ev.groupBy(F.window("ts", "10 minutes"), *keys).agg(*aggs)


def run(df, ck):
    q = (df.writeStream.format("noop").outputMode("update")
         .option("checkpointLocation", str(ck)).trigger(availableNow=True).start())
    q.awaitTermination()
    return q


drop(inbox, 1)
drop(inbox, 2)
run(query(), base)
drop(inbox, 3)

experiments = [
    ("the same query", {}),
    ("watermark delay 10 -> 30 minutes", {"delay": "30 minutes"}),
    ("a different filter", {"where": "error IS NULL AND page != '/'"}),
    ("group by page, not user_id", {"keys": ("page",)}),
    ("group by user_id and page", {"keys": ("user_id", "page")}),
    ("one more aggregate column", {"aggs": [F.count("*").alias("events"), F.max("ts").alias("last")]}),
    ("count -> sum of a different column", {"aggs": [F.sum(F.length("page")).alias("events")]}),
    ("a dedup operator before the aggregate", {"dedup": True}),
]
print(f"{'change on restart':<40} result")
for label, kw in experiments:
    ck = w / f"ck_{abs(hash(label))}"
    shutil.copytree(base, ck)
    try:
        q = run(query(**kw), ck)
        rows = sum(p.numInputRows for p in q.recentProgress)
        print(f"{label:<40} ran ({rows} rows)")
    except Exception as e:
        m = re.search(r"\[([A-Z_]+(?:\.[A-Z_]+)?)\]", str(e).split("terminated with exception:")[-1])
        print(f"{label:<40} {m.group(1) if m else type(e).__name__}")

print("\nThe page experiment again, then its state read back with the state data source:")
ck = w / "ck_page"
shutil.copytree(base, ck)
run(query(keys=("page",)), ck)
(spark.read.format("statestore").load(str(ck))
 .select(F.date_format("key.window.start", "HH:mm").alias("window"), F.col("key.user_id").alias("key.user_id"), "value.*")
 .orderBy("window", "`key.user_id`").show(30, truncate=False))

print("The key schema error, in full (first lines):")
ck = w / "ck_keys"
shutil.copytree(base, ck)
try:
    run(query(keys=("user_id", "page")), ck)
except Exception as e:
    msg = str(e).split("terminated with exception: ")[-1]
    for line in msg.splitlines()[:4]:
        print("  " + line[:150])

print("\nSwitch the session to RocksDB, then restart the HDFS-backed checkpoint:")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", ROCKSDB)
ck = w / "ck_rocks"
shutil.copytree(base, ck)
q = run(query(), ck)
last = max(int(f.name) for f in (ck / "offsets").iterdir() if f.name.isdigit())
used = re.search(r'"spark.sql.streaming.stateStore.providerClass":"([^"]+)"', (ck / f"offsets/{last}").read_text())
print("  session:     ", spark.conf.get("spark.sql.streaming.stateStore.providerClass").split(".")[-1])
print(f"  offsets/{last}:   ", used.group(1).split(".")[-1])
files = [p.name for p in (ck / "state").rglob("*") if p.is_file() and not p.name.endswith(".crc")]
print("  state files: ", sum(f.endswith(".delta") for f in files), ".delta files,",
      sum(f.endswith((".zip", ".sst", ".changelog")) for f in files), "RocksDB files")
