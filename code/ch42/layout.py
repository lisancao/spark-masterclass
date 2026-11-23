"""Chapter 42: partitionBy on disk, partition pruning, and the traps in folder names.

Run from code/:  uv run python ch42/layout.py
"""
import shutil, sys
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TRIPS, MONTH_START, MONTH_END

spark = session("ch42-layout", spark_sql_session_timeZone="UTC")
OUT = Path("/tmp/masterclass-ch42/layout")
shutil.rmtree(OUT, ignore_errors=True)

june = (spark.read.parquet(str(TRIPS))
        .where((F.col("tpep_pickup_datetime") >= MONTH_START) & (F.col("tpep_pickup_datetime") < MONTH_END))
        .withColumn("day", F.to_date("tpep_pickup_datetime")))
print("input partitions:", june.rdd.getNumPartitions())


def files_per_folder(path):
    c = Counter(p.parent.name for p in Path(path).rglob("*.parquet"))
    return len(c), sum(c.values()), min(c.values()), max(c.values())


WAYS = [
    ("plain write", lambda d: d.write),
    ("repartition(16) first", lambda d: d.repartition(16).write),
    ("repartition('day') first", lambda d: d.repartition("day").write),
    ("repartition('day'), maxRecordsPerFile 50000",
     lambda d: d.repartition("day").write.option("maxRecordsPerFile", 50000)),
]
print(f"\n{'how':<46}{'folders':>8}{'files':>7}{'min/folder':>12}{'max/folder':>12}")
for i, (label, w) in enumerate(WAYS):
    path = OUT / f"way{i}"
    w(june).partitionBy("day").parquet(str(path))
    n, total, lo, hi = files_per_folder(path)
    print(f"{label:<46}{n:>8}{total:>7}{lo:>12}{hi:>12}")

print("\nthe folders of the last layout:")
path = OUT / "way3"
for d in sorted(p for p in path.iterdir() if p.name.startswith("day="))[:3]:
    print(f"  {d.name}/  " + ", ".join(sorted(f.name[:10] + "...." + f.name.split(".", 1)[1] for f in d.glob("*.parquet"))))
print("  ...")

# Pruning: filters on the folder column remove files before Spark opens them.
back = spark.read.parquet(str(path))
print("\nschema read back:", ", ".join(f"{f.name}:{f.dataType.simpleString()}" for f in back.schema.fields[-3:]))


def show_filters(label, df):
    line = next(l for l in plan(df, "simple").splitlines() if "FileScan" in l)
    pf = line.split("PartitionFilters: ")[1].split("], ")[0] + "]"
    spark.sparkContext.setJobDescription(label)
    df.collect()
    q = [x for x in ui_api("sql?details=true&planDescription=false") if x["description"] == label][-1]
    metrics = {m["name"]: m["value"] for n in q["nodes"] if n["nodeName"].startswith("Scan")
               for m in n["metrics"]}
    print(f"  {label:<44} files read {metrics.get('number of files read', '?'):>4}   PartitionFilters: {pf[:70]}")


show_filters("day = 2026-06-15", back.where(F.col("day") == "2026-06-15").agg(F.sum("total_amount")))
show_filters("day >= 2026-06-25", back.where(F.col("day") >= "2026-06-25").agg(F.sum("total_amount")))
show_filters("to_date(pickup) = 2026-06-15",
             back.where(F.to_date("tpep_pickup_datetime") == "2026-06-15").agg(F.sum("total_amount")))
show_filters("dayofweek(day) = 1 (Sunday)", back.where(F.dayofweek("day") == 1).agg(F.sum("total_amount")))

# Folder names are text, and Spark guesses their types.
codes = OUT / "by_code"
spark.createDataFrame([("001", 1), ("010", 2), ("100", 3)], "code string, n int") \
    .write.partitionBy("code").parquet(str(codes))
print("\nfolders:", sorted(p.name for p in codes.iterdir() if p.is_dir()))
print("read back:", [tuple(r) for r in spark.read.parquet(str(codes)).orderBy("n").collect()],
      spark.read.parquet(str(codes)).schema.simpleString())
print("with a schema:", [tuple(r) for r in spark.read.schema("n int, code string").parquet(str(codes)).orderBy("n").collect()])
print("one folder alone:", spark.read.parquet(str(codes / "code=010")).columns,
      "| with basePath:", spark.read.option("basePath", str(codes)).parquet(str(codes / "code=010")).columns)
