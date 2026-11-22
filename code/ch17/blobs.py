"""Chapter 17, part 3: tables for blobs. What large BINARY values do to Parquet, a content-addressed
store with an inline threshold, the sources/blobs/derived design, MERGE traps, and a delete that must
remove bytes, on an Apache Iceberg table.

Iceberg has no release for Spark 4.3 yet. This script uses a runtime built from apache/iceberg
PR #18749 (commit b1eab64), at $BOOK_DATA/jars/iceberg-spark-runtime-4.3_2.13-b1eab64.jar.

Run from code/:  uv run python ch17/blobs.py
"""
import glob
import hashlib
import os
import re
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow.parquet as pq
from pyspark.sql import SparkSession, functions as F
from common.book import show, plan, ui_port, MESSY, WORK, DATA

JAR = DATA / "jars" / "iceberg-spark-runtime-4.3_2.13-b1eab64.jar"
TMP = WORK / "ch17" / "blobs"
shutil.rmtree(TMP, ignore_errors=True)
TMP.mkdir(parents=True)
spark = (SparkSession.builder.appName("ch17-blobs").master("local[*]").config("spark.ui.port", ui_port())
         .config("spark.jars", str(JAR))
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.lake.type", "hadoop")
         .config("spark.sql.catalog.lake.warehouse", str(TMP / "warehouse"))
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_0-9.]+)\]", str(e))
    return m.group(1) if m else f"{type(e).__name__}: {str(e).splitlines()[0][:80]}"


def attempt(label, fn):
    try:
        print(f"  {label:44} -> {fn()}")
    except Exception as e:
        print(f"  {label:44} -> {condition(e)}")


section("40 blobs of 1 MB, inline in Parquet")
import random
rnd = random.Random(17)          # random bytes do not compress, like JPEG, MP3 or zip files
blobs = spark.createDataFrame([(f"scan_{i:02d}.bin", rnd.randbytes(1024 * 1024), i % 4) for i in range(40)],
                              "name STRING, content BINARY, shelf INT")
inline = str(TMP / "inline")
blobs.coalesce(1).write.parquet(inline)
f = glob.glob(f"{inline}/*.parquet")[0]
md = pq.ParquetFile(f).metadata
print(f"file {os.path.getsize(f) / 1e6:.1f} MB, row groups: {md.num_row_groups}, rows: {md.num_rows}")
rg = md.row_group(0)
for i in range(rg.num_columns):
    c = rg.column(i)
    st = c.statistics
    print(f"  {c.path_in_schema:8} {c.total_compressed_size / 1e6:8.3f} MB compressed,"
          f" {c.total_uncompressed_size / 1e6:8.3f} MB raw, min/max: {'yes' if st is not None and st.has_min_max else 'no'}")
df = spark.read.parquet(inline)
rows_per_task = [r["count"] for r in df.groupBy(F.spark_partition_id().alias("t")).count().collect()]
print("scan tasks:", df.rdd.getNumPartitions(), " tasks that got rows:", len(rows_per_task), " rows in that task:", rows_per_task)
print("ReadSchema of a query on name and shelf:",
      next(l.split(": ", 1)[1] for l in plan(df.select("name", "shelf"), "formatted").splitlines() if "ReadSchema" in l))

section("a content-addressed store: hash the bytes, keep large blobs as files")
INLINE_MAX = 64 * 1024
incoming = (spark.read.format("binaryFile").option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.{png,wav,pdf,eml}").load(str(MESSY)))
dup = spark.createDataFrame([("file:/incoming/copy_of_red.png", None, 0,
                              (MESSY / "images" / "products" / "red_16x8.png").read_bytes())],
                            "path STRING, modificationTime TIMESTAMP, length BIGINT, content BINARY")
big = spark.createDataFrame([(f"file:/incoming/big_{i}.bin", None, 200_000, rnd.randbytes(200_000)) for i in range(2)],
                            "path STRING, modificationTime TIMESTAMP, length BIGINT, content BINARY")
incoming = incoming.unionByName(dup).unionByName(big).withColumn("length", F.length("content"))
files = incoming.select(F.regexp_replace("path", r"^.*/messy/", "").alias("source"), "length", "content",
                        F.sha2("content", 256).alias("sha"))
CAS = TMP / "cas"


def put(rows):
    import os
    for r in rows:
        if r.length > INLINE_MAX:
            d = CAS / r.sha[:2]
            d.mkdir(parents=True, exist_ok=True)
            target = d / r.sha
            if not target.exists():
                tmp = d / f".{r.sha}.{os.getpid()}"
                tmp.write_bytes(r.content)
                os.replace(tmp, target)         # atomic: a reader never sees half a blob


files.where(F.col("length") > INLINE_MAX).dropDuplicates(["sha"]).foreachPartition(put)
print("files in:", files.count(), " distinct hashes:", files.select("sha").distinct().count(),
      " blobs in the store:", sum(1 for _ in CAS.rglob("*") if _.is_file()))
for p in sorted(CAS.rglob("*")):
    if p.is_file():
        print("  cas/" + str(p.relative_to(CAS))[:20] + "...", p.stat().st_size, "bytes")

section("three tables: sources, blobs, derived")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.media")
spark.sql("""CREATE TABLE lake.media.blobs (sha STRING, length BIGINT, kind STRING, inline BINARY, uri STRING)
             USING iceberg TBLPROPERTIES ('write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')""")
spark.sql("""CREATE TABLE lake.media.sources (source STRING, sha STRING, seen_at TIMESTAMP) USING iceberg
             TBLPROPERTIES ('write.delete.mode'='merge-on-read')""")
kind = (F.when(F.hex(F.substring("content", 1, 4)) == "89504E47", "png")
        .when(F.hex(F.substring("content", 1, 4)) == "52494646", "wav")
        .when(F.hex(F.substring("content", 1, 4)) == "25504446", "pdf")
        .when(F.hex(F.substring("content", 1, 4)) == "46726F6D", "email")
        .otherwise("other"))
batch = files.select("source", "sha", "length", kind.alias("kind"),
                     F.when(F.col("length") <= INLINE_MAX, F.col("content")).alias("inline"),
                     F.when(F.col("length") > INLINE_MAX,
                            F.concat(F.lit("cas/"), F.substring("sha", 1, 2), F.lit("/"), F.col("sha"))).alias("uri"))
batch.createOrReplaceTempView("batch")
spark.sql("INSERT INTO lake.media.blobs SELECT DISTINCT sha, length, kind, inline, uri FROM batch WHERE kind = 'png'")
attempt("MERGE with two source rows for one hash", lambda: spark.sql("""
    MERGE INTO lake.media.blobs t USING batch s ON t.sha = s.sha
    WHEN MATCHED THEN UPDATE SET t.length = s.length
    WHEN NOT MATCHED THEN INSERT (sha, length, kind, inline, uri) VALUES (s.sha, s.length, s.kind, s.inline, s.uri)
    """).count())
spark.sql("DELETE FROM lake.media.blobs")
spark.sql("""MERGE INTO lake.media.blobs t USING batch s ON t.sha = s.sha
             WHEN NOT MATCHED THEN INSERT (sha, length, kind, inline, uri) VALUES (s.sha, s.length, s.kind, s.inline, s.uri)""")
print("insert-only MERGE with duplicates in the source: rows", spark.table("lake.media.blobs").count(),
      " distinct hashes", spark.table("lake.media.blobs").select("sha").distinct().count())
spark.sql("DELETE FROM lake.media.blobs")
spark.sql("""MERGE INTO lake.media.blobs t
             USING (SELECT sha, any_value(length) AS length, any_value(kind) AS kind,
                           any_value(inline) AS inline, any_value(uri) AS uri FROM batch GROUP BY sha) s
             ON t.sha = s.sha
             WHEN NOT MATCHED THEN INSERT (sha, length, kind, inline, uri) VALUES (s.sha, s.length, s.kind, s.inline, s.uri)""")
spark.sql("INSERT INTO lake.media.sources SELECT source, sha, current_timestamp() FROM batch")
print("after dropDuplicates on the hash: blobs", spark.table("lake.media.blobs").count(),
      " sources", spark.table("lake.media.sources").count())
show(spark.sql("""SELECT b.kind, count(*) AS blobs, sum(b.length) AS bytes,
                         count_if(b.uri IS NOT NULL) AS in_store, count_if(b.inline IS NOT NULL) AS inline
                  FROM lake.media.blobs b GROUP BY b.kind ORDER BY b.kind"""))
show(spark.sql("""SELECT s.source, substr(s.sha, 1, 12) AS sha FROM lake.media.sources s
                  WHERE s.sha IN (SELECT sha FROM lake.media.sources GROUP BY sha HAVING count(*) > 1)
                  ORDER BY s.source"""))

section("derived rows, keyed by hash and extractor version")
spark.sql("""CREATE TABLE lake.media.derived (sha STRING, extractor STRING, version INT, value STRING) USING iceberg""")
todo = spark.sql("""SELECT b.sha FROM lake.media.blobs b
                    LEFT ANTI JOIN lake.media.derived d ON d.sha = b.sha AND d.extractor = 'kind' AND d.version = 1""")
print("work for extractor kind v1, first run:", todo.count())
spark.sql("INSERT INTO lake.media.derived SELECT sha, 'kind', 1, kind FROM lake.media.blobs")
print("work for extractor kind v1, second run:", todo.count())

section("forget one file: a delete leaves the bytes on disk")
target = spark.sql("SELECT sha FROM lake.media.sources WHERE source = 'mail/ticket_2.eml'").first().sha
marker = b"order 1002 has a problem"


def files_with(marker, root):
    hits = []
    for p in Path(root).rglob("*.parquet"):
        if marker in p.read_bytes():
            hits.append(p.name[:24] + "...")
    return hits


print("data files that hold the e-mail text before the delete:", len(files_with(marker, TMP / "warehouse")))
before = spark.sql("SELECT snapshot_id FROM lake.media.blobs.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]
spark.sql(f"DELETE FROM lake.media.blobs WHERE sha = '{target}'")
spark.sql(f"DELETE FROM lake.media.sources WHERE sha = '{target}'")
spark.sql(f"DELETE FROM lake.media.derived WHERE sha = '{target}'")
print("rows with the hash after DELETE:", spark.sql(f"SELECT count(*) FROM lake.media.blobs WHERE sha = '{target}'").first()[0])
print("data files that still hold the text:", len(files_with(marker, TMP / "warehouse")))
print("delete files in the table:", spark.sql("SELECT count(*) FROM lake.media.blobs.delete_files").first()[0])
old = before
print("time travel to the snapshot before the delete still finds it:",
      spark.sql(f"SELECT count(*) FROM lake.media.blobs VERSION AS OF {old} WHERE sha = '{target}'").first()[0])
for t in ("blobs", "sources", "derived"):
    spark.sql(f"CALL lake.system.rewrite_data_files(table => 'media.{t}', options => map('rewrite-all', 'true'))")
    spark.sql(f"CALL lake.system.rewrite_position_delete_files(table => 'media.{t}', options => map('rewrite-all', 'true'))")
print("after rewrite_data_files, files that hold the text:", len(files_with(marker, TMP / "warehouse")))
for t in ("blobs", "sources", "derived"):
    spark.sql(f"CALL lake.system.expire_snapshots(table => 'media.{t}', older_than => current_timestamp(), retain_last => 1)")
print("after expire_snapshots, files that hold the text:", len(files_with(marker, TMP / "warehouse")))
attempt("time travel to the snapshot before the delete", lambda: spark.sql(
    f"SELECT count(*) FROM lake.media.blobs VERSION AS OF {old}").first()[0])
print("the blob in the store is a separate file: remove it too, with the hash:", f"cas/{target[:2]}/{target[:12]}...",
      "(this e-mail is small, so it was inline)")
