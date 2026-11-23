"""Chapter 31: the reader and the writer. Save modes, partitioned layouts, formats compared, small files.

Run from code/:  flock $BOOK_DATA/.bench.lock uv run python ch31/formats.py
"""
import os
import shutil
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, show, TRIPS, MESSY, WORK

spark = session("ch31-formats", spark_sql_session_timeZone="UTC")
out = WORK / "ch31" / "formats"
shutil.rmtree(out, ignore_errors=True)


def section(title):
    print(f"\n== {title} ==")


def du(path):
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())


def tree(path, limit=12):
    files = sorted(str(f.relative_to(path)) for f in Path(path).rglob("*") if f.is_file())
    for f in files[:limit]:
        print("  " + f)
    if len(files) > limit:
        print(f"  ... {len(files) - limit} more")


# The first million rows of the month, picked by row index so every run writes the same rows.
trips = spark.read.parquet(str(TRIPS))
sample = trips.where(F.col("_metadata.row_index") < 1_000_000).drop("_metadata")

section("a write is a folder of part files")
dest = out / "parquet"
sample.write.parquet(str(dest))
tree(dest)
try:
    sample.write.parquet(str(dest))
except Exception as e:
    print("write again, default mode:", str(e).split("]")[0] + "]")
for mode in ("ignore", "append", "overwrite"):
    sample.limit(10).write.mode(mode).parquet(str(dest))
    print(f"mode={mode:9} -> {spark.read.parquet(str(dest)).count():>9,} rows")

section("partitionBy: one folder per value")
by_pay = out / "by_payment_type"
(sample.write.partitionBy("payment_type").parquet(str(by_pay)))
tree(by_pay, 20)
cash = spark.read.parquet(str(by_pay)).where("payment_type = 2")
scan = [l for l in plan(cash, "formatted").splitlines()
        if l.startswith(("PartitionFilters", "Location"))]
print("\n".join(l.replace(str(out), "work/ch31/formats") for l in scan))
print("cash trips:", f"{cash.count():,}")

section("query a file without a table")
spark.sql(f"SELECT payment_type, count(*) AS trips FROM parquet.`{by_pay}` "
          "GROUP BY payment_type ORDER BY payment_type").show()

section("the same million trips in five formats")
writers = {
    "csv": lambda df, p: df.write.option("header", True).csv(p),
    "csv.gz": lambda df, p: df.write.option("header", True).option("compression", "gzip").csv(p),
    "json": lambda df, p: df.write.json(p),
    "orc": lambda df, p: df.write.orc(p),
    "parquet": lambda df, p: df.write.parquet(p),
}
readers = {
    "csv": lambda p: spark.read.schema(sample.schema).option("header", True).csv(p),
    "csv.gz": lambda p: spark.read.schema(sample.schema).option("header", True).csv(p),
    "json": lambda p: spark.read.schema(sample.schema).json(p),
    "orc": lambda p: spark.read.orc(p),
    "parquet": lambda p: spark.read.parquet(p),
}
one = sample.coalesce(1)             # one file per format, so the sizes compare like for like
for fmt, w in writers.items():
    w(one, str(out / "cmp" / fmt))


def timed(fn, runs=7):
    fn()                                                    # warm-up
    t = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        t.append(time.perf_counter() - t0)
    return statistics.median(t)


print(f"{'format':8} {'size MB':>8} {'avg(fare) s':>12} {'avg(all) s':>11}  scan tasks")
for fmt, r in readers.items():
    p = str(out / "cmp" / fmt)
    df = r(p)
    one_col = timed(lambda: df.agg(F.avg("fare_amount")).collect())
    every = timed(lambda: df.agg(*[F.max(c) for c in df.columns]).collect())
    tasks = df.rdd.getNumPartitions()
    print(f"{fmt:8} {du(p) / 1e6:8.1f} {one_col:12.3f} {every:11.3f}  {tasks}")

section("small files: how many tasks does a folder of tiny files become?")
tiny = out / "tiny"
tiny.mkdir(parents=True)
src = (MESSY / "events" / "events_2026-10-01.json").read_bytes()
for i in range(2000):
    (tiny / f"part-{i:05d}.json").write_bytes(src)
print(f"2000 files, {du(tiny) / 1e6:.1f} MB")
for open_cost in ("4MB", "64KB", "1KB"):
    spark.conf.set("spark.sql.files.openCostInBytes", open_cost)
    df = spark.read.schema("event_type STRING").json(str(tiny))
    print(f"openCostInBytes={open_cost:5} -> {df.rdd.getNumPartitions():4} tasks,"
          f" read in {timed(lambda: df.count(), runs=3):.2f} s")
spark.conf.unset("spark.sql.files.openCostInBytes")
print("run conditions:", os.cpu_count(), "cores, local[*], median of 7 runs (3 for small files) after a warm-up")
