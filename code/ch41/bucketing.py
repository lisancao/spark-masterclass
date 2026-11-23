"""Chapter 41: bucketed tables. The layout on disk, the joins and aggregates without a shuffle, bucket pruning.

Run from code/:  uv run python ch41/bucketing.py   (timings under the bench lock)
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from c86 import start, plan, median_s, noop, last_sql_metrics, ui_api, WH, F


def scan_input():
    """Tasks of the last SQL query that read any bytes, and the bytes they read (REST API)."""
    ex = max(ui_api("sql?details=false&length=1000"), key=lambda e: e["id"])
    n = used = total = 0
    for j in ex["successJobIds"]:
        for st in ui_api(f"jobs/{j}")["stageIds"]:
            for att in ui_api(f"stages/{st}"):
                if att["status"] != "COMPLETE":
                    continue
                for t in ui_api(f"stages/{st}/{att['attemptId']}/taskList?length=1000"):
                    b = t.get("taskMetrics", {}).get("inputMetrics", {}).get("bytesRead", 0)
                    n += 1
                    used += b > 0
                    total += b
    return n, used, total

spark = start("ch41-bucketing", spark_driver_memory="8g", spark_sql_autoBroadcastJoinThreshold="-1")
for t in ("trips_b16", "trips_b16_many", "trips_plain", "zstats_b16", "zstats_b8", "zstats_plain"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")

trips = spark.table("trips").join(spark.table("fares"), "trip_id")
zstats = trips.groupBy("PULocationID").agg(F.count("*").alias("n"), F.avg("total_amount").alias("avg_amount"))


def layout(table):
    files = [p for p in (WH / table).glob("*.parquet")]
    ids = sorted({re.search(r"_(\d{5})\.c000", p.name).group(1) for p in files if re.search(r"_(\d{5})\.c000", p.name)})
    return len(files), ids


print("=== 1. write: bucketBy without and with a repartition on the bucket column")
trips.write.bucketBy(16, "PULocationID").sortBy("PULocationID").saveAsTable("trips_b16_many")
(trips.repartition(16, "PULocationID").write.bucketBy(16, "PULocationID").sortBy("PULocationID")
      .saveAsTable("trips_b16"))
trips.write.saveAsTable("trips_plain")
zstats.write.bucketBy(16, "PULocationID").saveAsTable("zstats_b16")
zstats.write.bucketBy(8, "PULocationID").saveAsTable("zstats_b8")
zstats.write.saveAsTable("zstats_plain")
for t in ("trips_b16_many", "trips_b16", "trips_plain"):
    n, ids = layout(t)
    print(f"  {t:15} {n:4} files, bucket ids in the names: {len(ids)} distinct" + (f" ({ids[0]} to {ids[-1]})" if ids else ""))
print("  one file name: " + sorted(p.name for p in (WH / "trips_b16").glob("*.parquet"))[3])
info = {r.col_name: r.data_type for r in spark.sql("DESC EXTENDED trips_b16").collect()}
print(f"  DESC EXTENDED: Num Buckets {info.get('Num Buckets')}, Bucket Columns {info.get('Bucket Columns')}, "
      f"Sort Columns {info.get('Sort Columns')}")

for t in ("trips_b16", "trips_plain", "zstats_b16", "zstats_b8", "zstats_plain"):
    spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS")


def shape(q):
    p = plan(q, "simple").split("== Physical Plan ==")[-1]
    ex = re.findall(r"Exchange (hashpartitioning\([^)]*\)|SinglePartition|rangepartitioning)", p)
    ex = [re.sub(r"#\d+", "", e) for e in ex]
    j = re.findall(r"(SortMergeJoin|BroadcastHashJoin|ShuffledHashJoin)", p)
    return j, ex


JOIN = "SELECT z.n, t.total_amount FROM {t} t JOIN {z} z ON t.PULocationID = z.PULocationID"
print("\n=== 2. the same join on four pairs of tables (broadcast off)")
times = {}
cases = [("plain   x plain", "trips_plain", "zstats_plain", {}),
         ("b16     x b16", "trips_b16", "zstats_b16", {}),
         ("b16     x b8", "trips_b16", "zstats_b8", {}),
         ("b16     x b8, coalesce buckets", "trips_b16", "zstats_b8", {"spark.sql.bucketing.coalesceBucketsInJoin.enabled": "true"}),
         ("b16     x b16, bucketing off", "trips_b16", "zstats_b16", {"spark.sql.sources.bucketing.enabled": "false"})]
for label, t, z, conf in cases:
    for k, v in conf.items():
        spark.conf.set(k, v)
    sql = JOIN.format(t=t, z=z)
    j, ex = shape(spark.sql(sql))
    times[label] = median_s(lambda: noop(spark.sql(sql)))
    print(f"  {label:32} {j[0]:14} exchanges {len(ex)}  {times[label]:.2f} s")
    for e in ex:
        print(f"  {'':32}   Exchange {e}")
    for k in conf:
        spark.conf.unset(k)
print(f"  plain / b16 x b16: {times['plain   x plain'] / times['b16     x b16']:.1f}x")

print("\n=== 3. an aggregate on the bucket key")
for t in ("trips_plain", "trips_b16"):
    q = spark.table(t).groupBy("PULocationID").agg(F.sum("total_amount"))
    j, ex = shape(q)
    print(f"  {t:12} exchanges {len(ex)} {ex}")

print("\n=== 4. bucket pruning: a filter on the bucket column")
for auto in ("true", "false"):
    spark.conf.set("spark.sql.sources.bucketing.autoBucketedScan.enabled", auto)
    q = spark.table("trips_b16").where("PULocationID = 132").agg(F.count("*"))
    scan = [l.strip() for l in plan(q, "formatted").splitlines() if "SelectedBucketsCount" in l or "Bucketed:" in l]
    q.collect()
    m = [mm for _, mm in last_sql_metrics(spark, "Scan parquet")][0]
    n, used, b = scan_input()
    print(f"  autoBucketedScan.enabled={auto}: {' | '.join(scan)}")
    print(f"    metric 'number of files read' {m.get('number of files read')}, rows out of scan {m.get('number of output rows')}")
    print(f"    tasks of the query {n}, tasks that read input {used}, input bytes {b:,}")
spark.conf.unset("spark.sql.sources.bucketing.autoBucketedScan.enabled")

print("\n=== 5. a query that gains nothing from buckets: the planner turns the bucketed scan off")
q = spark.table("trips_b16").where("total_amount > 100").agg(F.count("*"))
print("  " + [l.strip() for l in plan(q, "formatted").splitlines() if "Bucketed:" in l][0])
