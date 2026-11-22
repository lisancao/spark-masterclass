"""Chapter 25: what is in the bytes. Lazy content reads, magic bytes, a PNG header in SQL, hashes.

Run from code/:  uv run python ch25/bytes.py   (run ch25/listing.py first: it makes the uploads folder)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, show, MESSY, WORK

spark = session("ch25-bytes", spark_sql_session_timeZone="UTC")


def section(title):
    print(f"\n== {title} ==")


def head_hex(start, n):
    """Hex of n bytes of `content` from byte `start` (substring on binary counts bytes from 1)."""
    return F.hex(F.substring("content", start, n))


def be_uint32(start):
    """The 4-byte big-endian unsigned integer at byte `start`."""
    return F.conv(head_hex(start, 4), 16, 10).cast("long")


PNG_MAGIC, IHDR = "89504E470D0A1A0A", "49484452"
roots = [str(MESSY / d) for d in ("images", "audio", "pdf", "mail")] + [str(WORK / "ch25/uploads")]
files = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(roots)

section("content is read only when a query asks for it")
big = files.where(F.col("length") > 5000).select("path", "length")
print("\n".join(l for l in plan(big, "simple").splitlines() if "FileScan" in l)
      .replace(", ", ",\n    ").replace(str(MESSY), "data/messy"))
spark.conf.set("spark.sql.sources.binaryFile.maxLength", "1000")
print("maxLength=1000, select(path, length).count():", files.select("path", "length").count())
try:
    files.select(F.sha2("content", 256)).collect()
except Exception as e:
    msg = str(e)
    import re
    print("maxLength=1000, select(sha2(content)):", re.search(r"\[[A-Z_.]+\]", msg).group(0))
    cause = re.search(r"The length of (\S+) is (\d+), which exceeds the max length allowed: (\d+)", msg)
    print("  caused by: The length of", cause.group(1).rsplit("/", 1)[1], "is", cause.group(2) + ",",
          "which exceeds the max length allowed:", cause.group(3))
spark.conf.unset("spark.sql.sources.binaryFile.maxLength")

section("magic bytes, not file names")
kind = (F.when(head_hex(1, 8) == PNG_MAGIC, "png")
        .when((head_hex(1, 4) == "52494646") & (head_hex(9, 4) == "57415645"), "wav")
        .when(head_hex(1, 5) == "255044462D", "pdf")
        .otherwise("other"))
png_ok = F.when(kind == "png", head_hex(13, 4) == IHDR)
inv = (files
       .withColumn("kind", kind).withColumn("png_ok", png_ok)
       .select(F.col("_metadata.file_name").alias("file"), "length", "kind", "png_ok",
               head_hex(1, 8).alias("first_8_bytes"),
               F.when(F.col("png_ok"), be_uint32(17)).alias("width"),
               F.when(F.col("png_ok"), be_uint32(21)).alias("height"),
               F.sha2("content", 256).alias("sha256"))
       .cache())
show(inv.drop("sha256").orderBy("kind", "file"), 30)
show(inv.groupBy("kind").count().orderBy("kind"))

section("decode a header without checking it first")
corrupt = files.where(F.col("path").endswith("corrupt.png"))
show(corrupt.select(head_hex(13, 4).alias("bytes 13-16"),
                    F.substring("content", 9, 16).cast("string").alias("bytes 9-24 as text"),
                    be_uint32(17).alias("width"), be_uint32(21).alias("height")))

section("duplicates by SHA-256")
show(inv.groupBy("sha256").agg(F.count("*").alias("n"),
                               F.sort_array(F.collect_list("file")).alias("files"))
     .where("n > 1").select(F.substring("sha256", 1, 12).alias("sha256 (first 12)"), "n", "files"))

section("binaryFile cannot write")
try:
    files.write.format("binaryFile").save(str(WORK / "ch25/out"))
except Exception as e:
    print(str(e).splitlines()[0][:100])
out = WORK / "ch25/blobs.parquet"
files.select("path", "length", "content").write.mode("overwrite").parquet(str(out))
print("as Parquet instead:", spark.read.parquet(str(out)).schema.simpleString())
