"""Chapter 86: join hints and partitioning hints, what each one puts in the plan, and which hint wins.

Reads the Parquet folders of the stats.py tables as temporary views (no metastore needed).
Run from code/:  uv run python ch86/hints.py 2> scratch.log ; grep -i hint scratch.log  (the hint warnings)
"""
import re
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from c86 import plan, WH, ROOT, F
from common.book import session

spark = session("ch86-hints", spark_sql_session_timeZone="UTC", spark_ui_showConsoleProgress="false")
spark.read.parquet(str(WH / "trips")).createOrReplaceTempView("trips")
spark.read.parquet(str(WH / "fares")).createOrReplaceTempView("fares")
spark.read.parquet(str(WH / "zones")).createOrReplaceTempView("zones")


def phys(q):
    return plan(q, "simple").split("== Physical Plan ==")[-1]


def join_of(q):
    m = re.findall(r"(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin|BroadcastNestedLoopJoin|CartesianProduct)[^\n]*?(BuildLeft|BuildRight)?", phys(q))
    return " ".join(x for x in m[0] if x) if m else "?"


Z = "SELECT {h} z.Borough, count(*) FROM trips t JOIN zones z ON t.PULocationID = z.LocationID GROUP BY z.Borough"
print("=== 1. join hints in SQL (trips JOIN zones; zones is 6 KB, so the default is a broadcast)")
for h in ["", "BROADCAST(t)", "MERGE(z)", "SHUFFLE_MERGE(z)", "SHUFFLE_HASH(z)", "SHUFFLE_REPLICATE_NL(z)", "MAPJOIN(z)"]:
    print(f"  {h or '(no hint)':26} {join_of(spark.sql(Z.format(h=f'/*+ {h} */' if h else '')))}")

print("\n=== 2. the same hints on a DataFrame")
t, z = spark.table("trips"), spark.table("zones")
for h in ["broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]:
    q = t.join(z.hint(h), t.PULocationID == z.LocationID)
    print(f"  {f'z.hint({h!r})':26} {join_of(q)}")
print(f"  {'F.broadcast(t)':26} {join_of(z.join(F.broadcast(t), t.PULocationID == z.LocationID))}")

print("\n=== 3. two hints in one query: which one wins")
for h in ["BROADCAST(t), MERGE(z)", "MERGE(t), SHUFFLE_HASH(z)", "SHUFFLE_HASH(t), SHUFFLE_REPLICATE_NL(z)", "BROADCAST(t), BROADCAST(z)",
          "SHUFFLE_HASH(t), SHUFFLE_HASH(z)"]:
    print(f"  {h:42} {join_of(spark.sql(Z.format(h=f'/*+ {h} */')))}")

print("\n=== 4. hints that the planner cannot use (it ignores them and logs a warning)")
spark.sparkContext.setLogLevel("WARN")
FULL = "SELECT /*+ BROADCAST(t) */ count(*) FROM trips t FULL OUTER JOIN zones z ON t.PULocationID = z.LocationID"
print(f"  BROADCAST(t) on a full outer join:   {join_of(spark.sql(FULL))}")
NONEQ = "SELECT /*+ SHUFFLE_HASH(z) */ count(*) FROM trips t JOIN zones z ON t.PULocationID < z.LocationID"
print(f"  SHUFFLE_HASH(z) on a non-equi join:  {join_of(spark.sql(NONEQ))}")
print(f"  BROADCAST(x), no table x:            {join_of(spark.sql(Z.format(h='/*+ BROADCAST(x) */')))}")
print(f"  BROADKAST(z), a misspelled name:     {join_of(spark.sql(Z.format(h='/*+ BROADKAST(z) */')))}")
spark.sparkContext.setLogLevel("ERROR")

print("\n=== 5. partitioning hints: the Exchange that each one adds, and the files of a write")
OUT = ROOT / "data" / "work" / "ch86-hints"
P = "SELECT {h} * FROM trips"


def exch(q):
    p = phys(q)
    e = re.findall(r"(Exchange [^\n]*|Coalesce \d+)", p)
    e = [re.sub(r"#\d+L?", "", re.sub(r", \[plan_id=\d+\]", "", x)).strip() for x in e]
    return e[0] if e else "(no Exchange)"


rows = []
for h, conf in [("", {}), ("COALESCE(2)", {}), ("REPARTITION(4)", {}), ("REPARTITION(4, PULocationID)", {}),
                ("REPARTITION(PULocationID)", {}), ("REPARTITION_BY_RANGE(4, trip_distance)", {}),
                ("REBALANCE", {}), ("REBALANCE(PULocationID)", {}),
                ("REBALANCE", {"spark.sql.adaptive.coalescePartitions.parallelismFirst": "false"}),
                ("REBALANCE", {"spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
                               "spark.sql.adaptive.advisoryPartitionSizeInBytes": "16m"})]:
    for k, v in conf.items():
        spark.conf.set(k, v)
    q = spark.sql(P.format(h=f"/*+ {h} */" if h else ""))
    shutil.rmtree(OUT, ignore_errors=True)
    q.write.parquet(str(OUT))
    files = sorted(OUT.glob("*.parquet"))
    sizes = [f.stat().st_size / 1048576 for f in files]
    label = h or "(no hint)"
    if conf:
        label += ", parallelismFirst off" + (", 16 MB" if len(conf) > 1 else "")
    print(f"  {label:42} files {len(files):3}  MB min {min(sizes):5.1f} max {max(sizes):5.1f}")
    print(f"  {'':42} {exch(q)[:95]}")
    for k in conf:
        spark.conf.unset(k)

print("\n=== 6. the DataFrame forms")
t = spark.table("trips")
for label, q in [("t.coalesce(2)", t.coalesce(2)), ("t.repartition(4, 'PULocationID')", t.repartition(4, "PULocationID")),
                 ("t.repartitionByRange(4, 'trip_distance')", t.repartitionByRange(4, "trip_distance")),
                 ("t.hint('rebalance', 'PULocationID')", t.hint("rebalance", "PULocationID"))]:
    print(f"  {label:42} {exch(q)[:90]}")
