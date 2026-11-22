"""Steps 4-5: the RocksDB state store on disk. The same per-user count three times:
changelog checkpointing off (the default), on, and with state checkpoint format v2.
Then the column families of the transformWithState checkpoint from Chapter 26.

Run from code/ (after ch26/t2_sessions.py):  uv run python ch28/c2_rocksdb.py
"""
import json
import sys
import zipfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ch26"))

from common.book import session, work, tree, ROCKSDB, ROOT
from checkout import drop, SCHEMA

spark = session("ch28-rocksdb", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC",
                spark_sql_streaming_stateStore_providerClass=ROCKSDB)
CL = "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled"
FV = "spark.sql.streaming.stateStore.checkpointFormatVersion"


def run(name, conf):
    for k, v in conf.items():
        spark.conf.set(k, v)
    w = work(f"ch28/c2_{name}")
    (w / "inbox").mkdir()
    for i in range(1, 5):
        drop(w / "inbox", i)
    q = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(w / "inbox"))
         .groupBy("user").count().writeStream.format("noop").outputMode("update")
         .option("checkpointLocation", str(w / "ck")).trigger(availableNow=True).start())
    q.awaitTermination()
    for k in conf:
        spark.conf.unset(k)
    return w / "ck"


print(f"1. {CL} = {spark.conf.get(CL, 'false (default)')}")
ck = run("zip", {})
print(tree(ck / "state/0/0"))
z = ck / "state/0/0/4.zip"
print(f"\n4.zip holds: {sorted(zipfile.ZipFile(z).namelist())}")
meta = zipfile.ZipFile(z).read("metadata").decode().splitlines()
print("its metadata file:", meta[0])
m = json.loads(meta[1])
print(json.dumps({k: m[k] for k in m if k in ("sstFiles", "numKeys", "columnFamilyMapping")}, indent=1)[:900])

print(f"\n2. {CL} = true")
ck = run("changelog", {CL: "true"})
print(tree(ck / "state/0/0"))

print(f"\n3. {CL} = true and {FV} = 2")
ck = run("v2", {CL: "true", FV: "2"})
print(tree(ck / "state/0/0"))
print("commits/3:", (ck / "commits/3").read_text().strip().replace("\n", " | ")[:400])

print("\n4. The transformWithState checkpoint of Chapter 26 (t2_sessions.py), partition 0:")
tws = ROOT / "data/work/ch26/t2/checkpoint/state/0/0"
latest = max(tws.glob("*.zip"), key=lambda p: int(p.stem))
m = json.loads(zipfile.ZipFile(latest).read("metadata").decode().splitlines()[1])
print(f"  {latest.name}: numKeys {m['numKeys']}")
print("  columnFamilyMapping:", json.dumps(m["columnFamilyMapping"], sort_keys=True))
print("  state schema files:", sorted(str(p.relative_to(tws.parents[1])) for p in (tws.parents[1]).glob("_stateSchema/**/*") if p.is_file() and not p.name.endswith(".crc")))
