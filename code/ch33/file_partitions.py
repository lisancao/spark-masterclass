"""Chapter 33: how many partitions a file read makes, predicted from Spark's rule and then measured.

Classic only: it reads df.rdd.getNumPartitions(). Run from code/:  uv run python ch33/file_partitions.py
"""
import shutil, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, TRIPS, TAXI

spark = session("ch33-file-partitions")
CORES = spark.sparkContext.defaultParallelism
H1 = TAXI / "yellow-h1"                      # January to June 2026, one file per month
OUT = Path("/tmp/masterclass-ch33")
shutil.rmtree(OUT, ignore_errors=True)
MB = 1024 * 1024


def predict(sizes, splittable=True, max_bytes=128 * MB, open_cost=4 * MB, min_parts=None, max_num=None):
    """FilePartition.maxSplitBytes and getFilePartitions, in Python."""
    total = sum(s + open_cost for s in sizes)
    max_split = min(max_bytes, max(open_cost, total // (min_parts or CORES)))
    parts = pack(sizes, splittable, max_split, open_cost)
    if max_num and parts > max_num:          # rescale: ignore maxPartitionBytes, aim at max_num
        max_split = -(-total // max_num)
        parts = pack(sizes, splittable, max_split, open_cost)
    return parts, max_split


def pack(sizes, splittable, max_split, open_cost):
    pieces = []
    for s in sizes:
        if splittable:
            pieces += [min(max_split, s - off) for off in range(0, s, max_split)]
        else:
            pieces.append(s)
    pieces.sort(reverse=True)                # "Next Fit Decreasing"
    parts, current = 0, None
    for p in pieces:
        if current is None or current + p > max_split:
            parts, current = parts + 1, 0
        current += p + open_cost
    return parts


def files(path, pattern="*"):
    return sorted(f for f in Path(path).rglob(pattern) if f.is_file() and not f.name.startswith(("_", ".")))


# Inputs: the taxi months as they are, plus copies written in other layouts.
june = spark.read.parquet(str(TRIPS))
june.repartition(200).write.parquet(str(OUT / "small_files"))
sample = june.limit(500_000)
sample.coalesce(1).write.option("header", True).csv(str(OUT / "csv_plain"))
sample.coalesce(1).write.option("header", True).option("compression", "gzip").csv(str(OUT / "csv_gzip"))
sample.coalesce(1).write.option("header", True).option("compression", "bzip2").csv(str(OUT / "csv_bzip2"))

CASES = [
    ("June: 1 Parquet file", "parquet", TRIPS, {}),
    ("June, maxPartitionBytes 2MB", "parquet", TRIPS, {"spark.sql.files.maxPartitionBytes": str(2 * MB)}),
    ("January to June: 6 files", "parquet", H1, {}),
    ("January to June, minPartitionNum 64", "parquet", H1, {"spark.sql.files.minPartitionNum": "64"}),
    ("June as 200 small files", "parquet", OUT / "small_files", {}),
    ("200 small files, openCostInBytes 1", "parquet", OUT / "small_files",
     {"spark.sql.files.openCostInBytes": "1"}),
    ("200 small files, maxPartitionNum 4", "parquet", OUT / "small_files",
     {"spark.sql.files.maxPartitionNum": "4"}),
    ("CSV, 500,000 rows", "csv", OUT / "csv_plain", {}),
    ("the same CSV, gzip", "csv", OUT / "csv_gzip", {}),
    ("the same CSV, bzip2", "csv", OUT / "csv_bzip2", {}),
]
print(f"local[*] with {CORES} cores; defaults: maxPartitionBytes 128MB, openCostInBytes 4MB\n")
print(f"{'input':<38}{'files':>6}{'MB':>8}{'max split':>11}{'predicted':>11}{'actual':>8}")
for label, fmt, path, conf in CASES:
    for k, v in conf.items():
        spark.conf.set(k, v)
    fs = files(path, "*.parquet" if fmt == "parquet" else "part-*")
    sizes = [f.stat().st_size for f in fs]
    splittable = not str(fs[0]).endswith(".gz")
    kw = {"max_bytes": int(conf.get("spark.sql.files.maxPartitionBytes", 128 * MB)),
          "open_cost": int(conf.get("spark.sql.files.openCostInBytes", 4 * MB)),
          "min_parts": int(conf["spark.sql.files.minPartitionNum"]) if "spark.sql.files.minPartitionNum" in conf else None,
          "max_num": int(conf["spark.sql.files.maxPartitionNum"]) if "spark.sql.files.maxPartitionNum" in conf else None}
    predicted, max_split = predict(sizes, splittable, **kw)
    df = spark.read.option("header", True).format(fmt).load(str(path))
    actual = df.rdd.getNumPartitions()
    print(f"{label:<38}{len(fs):>6}{sum(sizes) / MB:>8.1f}{max_split / MB:>10.1f}M{predicted:>11}{actual:>8}")
    for k in conf:
        spark.conf.unset(k)

# For Parquet, a split only does work if it holds the midpoint of a row group.
import pyarrow.parquet as pq
print("\nrow groups: June", pq.ParquetFile(next(iter(files(TRIPS, "*.parquet")))).metadata.num_row_groups,
      "| January to June", sum(pq.ParquetFile(f).metadata.num_row_groups for f in files(H1, "*.parquet")))
for label, path in (("June", TRIPS), ("January to June", H1)):
    df = spark.read.parquet(str(path))
    busy = df.select(F.spark_partition_id().alias("p")).groupBy("p").count().count()
    print(f"{label}: {df.rdd.getNumPartitions()} partitions, {busy} of them return rows")
