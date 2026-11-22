"""Chapter 84: DataFrameWriter (save modes, saveAsTable, insertInto) against DataFrameWriterV2
(writeTo: create, append, overwrite, overwritePartitions, createOrReplace), on the session
catalog and on a V2 catalog (Iceberg).

Run from code/:  uv run python ch84/writers.py > ch84/output/writers.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402
from common.book import session, work, DATA  # noqa: E402
from ddl import runner, files, error_text  # noqa: E402

W = work("ch84-writers")
WH = W / "warehouse"
ICE = DATA / "jars" / "iceberg-spark-runtime-4.3_2.13-b1eab64.jar"
spark = session("ch84-writers", **{
    "spark.sql.warehouse.dir": str(WH),
    "spark.jars": str(ICE),
    "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake.type": "hadoop",
    "spark.sql.catalog.lake.warehouse": str(W / "lake")})
q = runner(spark)


def attempt(label, fn):
    try:
        fn()
        print(f"  {label:<44} ok")
    except Exception as e:  # noqa: BLE001
        print(f"  {label:<44} failed")
        print(error_text(e, 88, 2))


day1 = spark.createDataFrame([("2026-06-01", 1, 111546), ("2026-06-01", 2, 9000)],
                             "day STRING, vendor INT, trips BIGINT")
day2 = spark.createDataFrame([("2026-06-02", 1, 126260)], "day STRING, vendor INT, trips BIGINT")

print("== 1. save modes on a path")
out = W / "paths" / "daily"
day1.write.parquet(str(out))
print(f"  first write (default mode): rows at path: {spark.read.parquet(str(out)).count()}")
for mode in ["errorifexists", "append", "overwrite", "ignore"]:
    print(f"  -- mode {mode}")
    attempt(f"day1.write.mode('{mode}').parquet(out)", lambda: day1.write.mode(mode).parquet(str(out)))
    print(f"     rows at path: {spark.read.parquet(str(out)).count()}")

print("\n== 2. save modes on a table name (saveAsTable)")
day1.write.saveAsTable("daily")
print(f"  first saveAsTable('daily'): rows in daily: {spark.table('daily').count()}")
for mode in ["errorifexists", "append", "overwrite", "ignore"]:
    attempt(f"saveAsTable('daily', mode='{mode}')", lambda: day1.write.saveAsTable("daily", mode=mode))
    print(f"     rows in daily: {spark.table('daily').count()}")

print("\n== 3. insertInto matches columns by position, saveAsTable(append) by name")
swapped = day2.select("day", F.col("trips").cast("int").alias("trips"), "vendor")  # other order
print("  table columns:  ", spark.table("daily").columns)
print("  swapped columns:", swapped.columns)
attempt("swapped.write.insertInto('daily')", lambda: swapped.write.insertInto("daily"))
attempt("swapped.write.saveAsTable('daily', mode='append')",
        lambda: swapped.write.saveAsTable("daily", mode="append"))
q("SELECT * FROM daily ORDER BY day, vendor")
small = spark.createDataFrame([("2026-06-03", 7, "9")], "day STRING, vendor INT, trips STRING")
attempt("small.write.insertInto('daily') (STRING into BIGINT)", lambda: small.write.insertInto("daily"))

print("\n== 4. DataFrameWriterV2 on the session catalog (a V1 parquet table)")
attempt("day1.writeTo('daily2').using('parquet').create()",
        lambda: day1.writeTo("daily2").using("parquet").partitionedBy(F.col("day")).create())
attempt("day2.writeTo('daily2').append()", lambda: day2.writeTo("daily2").append())
attempt("day2.writeTo('daily2').overwritePartitions()", lambda: day2.writeTo("daily2").overwritePartitions())
attempt("day2.writeTo('daily2').createOrReplace()",
        lambda: day2.writeTo("daily2").using("parquet").createOrReplace())
print("  daily2 columns:", spark.table("daily2").columns, "(partition column last)")
attempt("day2.select(vendor, trips, day).write.insertInto",
        lambda: day2.select("vendor", "trips", "day").write.insertInto("daily2"))
q("SELECT * FROM daily2 ORDER BY day, vendor")

print("== 5. DataFrameWriterV2 on a V2 catalog (Iceberg)")
spark.sql("CREATE NAMESPACE lake.taxi")
T = "lake.taxi.daily"


def show(label):
    rows = spark.table(T).orderBy("day", "vendor").collect()
    snaps = spark.sql(f"SELECT operation FROM {T}.snapshots ORDER BY committed_at").collect()
    print(f"  {label:<26} rows: {[(r.day, r.vendor, r.trips) for r in rows]}")
    print(f"  {'':<26} last snapshot operation: {snaps[-1].operation}")


day1.writeTo(T).partitionedBy(F.col("day")).create()
show("create()")
day2.writeTo(T).append()
show("append()")
fix = spark.createDataFrame([("2026-06-01", 1, 111547)], "day STRING, vendor INT, trips BIGINT")
fix.writeTo(T).overwritePartitions()
show("overwritePartitions()")
fix.writeTo(T).overwrite(F.col("vendor") == 1)
show("overwrite(vendor = 1)")
day2.writeTo(T).createOrReplace()
show("createOrReplace()")
attempt("day2.writeTo(T).create() again", lambda: day2.writeTo(T).create())
q(f"SELECT snapshot_id IS NOT NULL AS has_id, operation FROM {T}.snapshots ORDER BY committed_at")
files(W / "lake" / "taxi" / "daily" / "metadata", "Iceberg metadata files", "*.json")
