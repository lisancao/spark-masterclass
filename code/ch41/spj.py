"""Chapter 41: a storage-partitioned join on two Iceberg tables. No Exchange before the join.

Uses the Iceberg runtime built for Spark 4.3 from apache/iceberg PR #18749 (see Chapter 55 and notes/ch55.md).
Run from code/:  uv run python ch41/spj.py   (timings under the bench lock)
"""
import re
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from c86 import trips_clean, plan, median_s, noop, DATA, ROOT, F

ICEBERG_JAR = DATA / "jars" / "iceberg-spark-runtime-4.3_2.13-b1eab64.jar"
LAKE = ROOT / "data" / "work" / "ch41-iceberg"
shutil.rmtree(LAKE, ignore_errors=True)

from pyspark.sql import SparkSession
import os
spark = (SparkSession.builder.appName("ch41-spj").master("local[*]")
         .config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040"))
         .config("spark.driver.memory", "8g")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.jars", str(ICEBERG_JAR))
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.lake.type", "hadoop")
         .config("spark.sql.catalog.lake.warehouse", f"file://{LAKE}")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version}, Iceberg jar {ICEBERG_JAR.name}")

trips = trips_clean(spark).select("PULocationID", "DOLocationID", "total_amount", "tip_amount")
zstats = trips.groupBy("PULocationID").agg(F.count("*").alias("n"), F.avg("tip_amount").alias("avg_tip"))
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.db")
spark.sql("""CREATE TABLE lake.db.trips (PULocationID INT, DOLocationID INT, total_amount DOUBLE, tip_amount DOUBLE)
             USING iceberg PARTITIONED BY (bucket(16, PULocationID))""")
spark.sql("""CREATE TABLE lake.db.zstats (PULocationID INT, n BIGINT, avg_tip DOUBLE)
             USING iceberg PARTITIONED BY (bucket(16, PULocationID))""")
spark.sql("""CREATE TABLE lake.db.zstats8 (PULocationID INT, n BIGINT, avg_tip DOUBLE)
             USING iceberg PARTITIONED BY (bucket(8, PULocationID))""")
trips.writeTo("lake.db.trips").append()
zstats.writeTo("lake.db.zstats").append()
zstats.writeTo("lake.db.zstats8").append()
for t in ("trips", "zstats", "zstats8"):
    n = spark.sql(f"SELECT count(*) FROM lake.db.{t}.files").first()[0]
    print(f"  lake.db.{t:8} data files {n}")

JOIN = "SELECT z.n, t.total_amount FROM lake.db.trips t JOIN lake.db.{z} z ON t.PULocationID = z.PULocationID"


def shape(q):
    p = plan(q, "formatted")
    phys = p.split("\n\n")[0]
    ex = len(re.findall(r"Exchange \(", phys))
    j = re.findall(r"(SortMergeJoin|ShuffledHashJoin|BroadcastHashJoin)", phys)
    gp = len(re.findall(r"GroupPartitions", phys))
    return j[0] if j else "?", ex, gp, phys


cases = [
    ("defaults", "zstats", {}),
    ("preserve-data-grouping", "zstats", {"spark.sql.iceberg.planning.preserve-data-grouping": "true"}),
    ("16 x 8 buckets, preserve-data-grouping", "zstats8", {"spark.sql.iceberg.planning.preserve-data-grouping": "true"}),
    ("16 x 8, + allowCompatibleTransforms", "zstats8", {"spark.sql.iceberg.planning.preserve-data-grouping": "true",
                                                       "spark.sql.sources.v2.bucketing.allowCompatibleTransforms.enabled": "true"}),
    ("v2 bucketing off", "zstats", {"spark.sql.iceberg.planning.preserve-data-grouping": "true",
                                    "spark.sql.sources.v2.bucketing.enabled": "false"}),
]
print("\n=== the same join, with each set of settings (broadcast off)")
shown = set()
for label, z, conf in cases:
    for k, v in conf.items():
        spark.conf.set(k, v)
    sql = JOIN.format(z=z)
    j, ex, gp, phys = shape(spark.sql(sql))
    t = median_s(lambda: noop(spark.sql(sql)))
    print(f"  {label:40} {j:14} Exchange nodes {ex}  {t:.2f} s")
    if label in ("defaults", "preserve-data-grouping"):
        print("\n".join("      " + l for l in phys.splitlines()[2:] if l.strip()))
    for k in conf:
        spark.conf.unset(k)
