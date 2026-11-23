"""Chapter 26, part 1: text and logs. Lines, whole files, regex fields, timestamps, stack traces,
a log that changes format, a quarantine that balances, and text that is not UTF-8.

Run from code/:  uv run python ch26/logs.py
"""
import gzip
import re
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, MESSY, WORK

spark = session("ch26-logs", spark_sql_session_timeZone="UTC")
LOGS = MESSY / "logs"
work = WORK / "ch26"
shutil.rmtree(work, ignore_errors=True)
work.mkdir(parents=True)


def section(title):
    print(f"\n== {title} ==")


def err(e):
    """The error condition and the first line of its message."""
    m = re.search(r"\[([A-Z_.]+)\]\s*(.*)", str(e))
    return f"[{m.group(1)}] {m.group(2)[:100]}" if m else str(e).splitlines()[0][:110]


section("the text source: one line, one row")
lines = spark.read.text(str(LOGS / "app.log"))
lines.printSchema()
print("app.log lines:", lines.count(), " scan tasks:", lines.rdd.getNumPartitions())
with gzip.open(work / "app.log.gz", "wb") as g:
    g.write((LOGS / "app.log").read_bytes())
gz = spark.read.text(str(work / "app.log.gz"))
print("app.log.gz lines:", gz.count(), "(decompressed by its extension)")
for mb in ("1KB", "128MB"):
    spark.conf.set("spark.sql.files.maxPartitionBytes", mb)
    print(f"maxPartitionBytes={mb:5}: app.log -> {spark.read.text(str(LOGS / 'app.log')).rdd.getNumPartitions():2} tasks,"
          f" app.log.gz -> {spark.read.text(str(work / 'app.log.gz')).rdd.getNumPartitions()} task")
spark.conf.unset("spark.sql.files.maxPartitionBytes")

section("wholetext, three ways")
p = str(LOGS / "app.log")
print('option("wholetext", True).text(p)          ->', spark.read.option("wholetext", True).text(p).count())
print("text(p, wholetext=True)                    ->", spark.read.text(p, wholetext=True).count())
print('format("text").option("wholetext", True)   ->',
      spark.read.format("text").option("wholetext", True).load(p).count())

section("regex fields from an access log")
ACCESS_RE = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)'
sample = spark.createDataFrame(
    [('10.0.2.238 - - [01/Oct/2026:10:00:00 +0000] "GET /?session=s1 HTTP/1.1" 404 7708 "-" "Mozilla/5.0"',),
     ("garbage line",)], "value STRING")
show(sample.select(F.regexp_extract("value", ACCESS_RE, 1).alias("ip"),
                   F.regexp_extract("value", ACCESS_RE, 2).alias("raw_ts"),
                   F.regexp_extract("value", ACCESS_RE, 4).alias("url"),
                   F.regexp_extract("value", ACCESS_RE, 6).try_cast("int").alias("status")))

section("what each regex function returns when nothing matches")
nomatch = sample.where("value = 'garbage line'")
r = nomatch.select(
    F.regexp_extract("value", r"status=(\d+)", 1).alias("regexp_extract"),
    F.regexp_extract_all("value", F.lit(r"status=(\d+)"), F.lit(1)).alias("regexp_extract_all"),
    F.regexp_substr("value", F.lit(r"status=\d+")).alias("regexp_substr"),
    F.regexp_count("value", F.lit(r"status=\d+")).alias("regexp_count"),
    F.col("value").rlike(r"status=\d+").alias("rlike")).first()
for k, v in r.asDict().items():
    print(f"  {k:20} -> {v!r}")
try:
    nomatch.select(F.regexp_extract("value", ACCESS_RE, 6).cast("int")).collect()
except Exception as e:
    print('  cast("int") on ""     ->', err(e))
try:
    nomatch.select(F.regexp_extract_all("value", r"status=(\d+)", 1)).collect()
except Exception as e:
    print("  pattern as a plain str ->", err(e))

section("timestamps: a pattern, a bad date, and try_")
bad = spark.createDataFrame([("01/Oct/2026:10:00:00 +0000",), ("31/Sep/2026:11:00:00 +0000",)], "raw STRING")
show(bad.select("raw", F.try_to_timestamp("raw", F.lit("dd/MMM/yyyy:HH:mm:ss Z")).alias("try_to_timestamp")))
try:
    bad.select(F.to_timestamp("raw", "dd/MMM/yyyy:HH:mm:ss Z")).collect()
except Exception as e:
    print("to_timestamp ->", err(e))

section("URLs: split_part and try_parse_url")
urls = spark.createDataFrame([("/api/orders/42?session=s9",), ("/cart?session=s12&x=1",), ("http://[bad",)], "url STRING")
show(urls.select("url", F.split_part("url", F.lit("?"), F.lit(1)).alias("path"),
                 F.try_parse_url("url", F.lit("QUERY"), F.lit("session")).alias("session")))

section("multi-line events: split whole files where an event starts")
EVENT_START = r"\n(?=\d{4}-\d{2}-\d{2} |\{)"
recs = (spark.read.text(p, wholetext=True)
        .select(F.explode(F.split("value", EVENT_START)).alias("rec")))
print("lines:", lines.count(), " events:", recs.count())
errors = recs.where(F.col("rec").contains("Exception"))
show(errors.select(
    F.regexp_extract("rec", r"\n([\w.$]+(?:Exception|Error)):", 1).alias("exception"),
    F.regexp_extract_all("rec", F.lit(r"\tat ([\w.$]+)\("), F.lit(1)).alias("frames")).limit(2))

section("a log that switches from text to JSON")
is_json = F.col("rec").startswith("{")
j = F.try_parse_json("rec")
events = recs.select(
    F.when(is_json, "json").otherwise("text").alias("fmt"),
    F.when(is_json, F.try_variant_get(j, "$.ts", "timestamp"))
     .otherwise(F.try_to_timestamp(F.regexp_extract("rec", r"^(\S+ \S+)", 1),
                                   F.lit("yyyy-MM-dd HH:mm:ss,SSS"))).alias("ts"),
    F.when(is_json, F.try_variant_get(j, "$.level", "string"))
     .otherwise(F.regexp_extract("rec", r"^\S+ \S+ (\w+)", 1)).alias("level"),
    F.when(is_json, F.try_variant_get(j, "$.ms", "int"))
     .otherwise(F.regexp_extract("rec", r"ms=(\d+)", 1).try_cast("int")).alias("ms"))
show(events.groupBy("fmt", "level").agg(F.count("*").alias("n"), F.round(F.avg("ms")).alias("avg_ms"),
                                        F.min("ts").alias("first"), F.max("ts").alias("last"))
     .orderBy("fmt", "level"))

section("quarantine: parsed + rejects = input")
extra = work / "access"
extra.mkdir()
shutil.copy(LOGS / "access.log", extra / "access.log")
(extra / "access_extra.log").write_text(
    '10.0.9.9 - - [01/Oct/2026:12:00:00 +0000] "GET /cart?session=s99 HTTP/1.1" 200 512 "-" "curl/8"\n'
    '10.0.9.9 - - [01/Oct/2026:12:00:01 +0000] "GET /ca\n'
    '10.0.9.9 - - [31/Sep/2026:12:00:02 +0000] "GET /cart?session=s99 HTTP/1.1" 200 512 "-" "curl/8"\n')
raw = spark.read.text(str(extra)).withColumn("file", F.col("_metadata.file_name"))
cand = raw.select("value", "file",
                  F.try_to_timestamp(F.regexp_extract("value", ACCESS_RE, 2),
                                     F.lit("dd/MMM/yyyy:HH:mm:ss Z")).alias("ts"),
                  F.regexp_extract("value", ACCESS_RE, 6).try_cast("int").alias("status"),
                  F.split_part(F.regexp_extract("value", ACCESS_RE, 4), F.lit("?"), F.lit(1)).alias("path"))
good = F.col("value").rlike(ACCESS_RE) & F.col("ts").isNotNull()
parsed, rejects = cand.where(good), cand.where(~good).select("file", "value")
n_in, n_ok, n_bad = raw.count(), parsed.count(), rejects.count()
print(f"input {n_in}, parsed {n_ok}, rejects {n_bad}, balanced: {n_in == n_ok + n_bad}")
show(rejects.select("file", F.substring("value", 1, 60).alias("value")))
show(parsed.groupBy("path").agg(F.count("*").alias("hits"),
                                F.sum((F.col("status") >= 500).cast("int")).alias("errors"))
     .orderBy(F.desc("errors"), "path").limit(5))

section("sessions from the access log")
show(parsed.groupBy(F.regexp_extract("value", r"^(\S+)", 1).alias("ip"),
                    F.session_window("ts", "30 seconds").alias("s"))
     .agg(F.count("*").alias("hits"))
     .select("ip", "s.start", "s.end", "hits").orderBy(F.desc("hits"), "ip").limit(3))

section("encodings: text decodes UTF-8, always")
lat = str(MESSY / "docs" / "notes_latin1.txt")
print("text                          ->", repr(spark.read.text(lat).first().value))
print("text, encoding=ISO-8859-1     ->", repr(spark.read.option("encoding", "ISO-8859-1").text(lat).first().value))
print("binaryFile + decode           ->", repr(spark.read.format("binaryFile").load(lat)
                                            .select(F.decode("content", "ISO-8859-1")).first()[0]))
show(spark.read.text(lat).select(F.is_valid_utf8("value").alias("is_valid_utf8"),
                                 F.make_valid_utf8("value").alias("make_valid_utf8")))
