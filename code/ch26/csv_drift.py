"""Chapter 26, part 3: bad rows and changing layouts. Parse modes, the corrupt record column,
DROPMALFORMED counts, a folder whose columns move, CSV inside a column, fixed width, and writing.

Run from code/:  uv run python ch26/csv_drift.py
"""
import csv
import glob
import re
import shutil
import sys
import zipfile
from functools import reduce
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, MESSY, WORK

spark = session("ch26-csv-drift", spark_sql_session_timeZone="UTC")
D = MESSY / "delimited"
tmp = WORK / "ch26" / "drift"
shutil.rmtree(tmp, ignore_errors=True)
tmp.mkdir(parents=True)


def section(title):
    print(f"\n== {title} ==")


def csv_file(name, text):
    (tmp / name).write_text(text)
    return str(tmp / name)


def cond(e):
    m = re.findall(r"\[([A-Z_.]+)\]", str(e))
    return " / ".join(dict.fromkeys(m)) if m else str(e).splitlines()[0][:100]


section("parse modes and the corrupt record column")
bad = csv_file("bad.csv", "id,amount,day\n1,2.5,2026-01-01\n2,abc,2026-01-02\n"
                          "3,4.0\n4,5.0,2026-01-04,EXTRA\n5,6.0,2026-02-30\n")
sch = "id INT, amount DOUBLE, day DATE"
show(spark.read.option("header", True).schema(sch).csv(bad))
raw = spark.read.option("header", True).schema(sch + ", _corrupt_record STRING").csv(bad)
show(raw)
try:
    raw.where(F.col("_corrupt_record").isNotNull()).count()
except Exception as e:
    print("count the corrupt rows ->", cond(e))
print("after cache()          ->", raw.cache().where(F.col("_corrupt_record").isNotNull()).count())
try:
    spark.read.option("header", True).schema(sch + ", _corrupt_record INT").csv(bad).collect()
except Exception as e:
    print("_corrupt_record INT    ->", cond(e))
try:
    spark.read.option("header", True).option("mode", "FAILFAST").schema(sch).csv(bad).collect()
except Exception as e:
    print("mode=FAILFAST          ->", cond(e))

section("DROPMALFORMED: three different answers")
dm = spark.read.option("header", True).option("mode", "DROPMALFORMED").schema(sch).csv(bad)
print("len(collect())          ", len(dm.collect()))
print("count()                 ", dm.count())
print('select("id").count()    ', dm.select("id").count())
spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
print("count(), pruning off    ",
      spark.read.option("header", True).option("mode", "DROPMALFORMED").schema(sch).csv(bad).count())
spark.conf.unset("spark.sql.csv.parser.columnPruning.enabled")
print("count() after cache()   ", dm.cache().count())

section("a folder whose columns move")
folder = str(D / "sales")
for f in sorted(Path(folder).glob("*.csv")):
    print(f"  {f.name}: {f.read_text().splitlines()[0]}")
naive = spark.read.option("header", True).csv(folder).withColumn("file", F.col("_metadata.file_name"))
show(naive.where(F.col("file").isin("sales_2026-10-02.csv")).limit(2))
try:
    spark.read.option("header", True).option("enforceSchema", False).schema(
        "id STRING, amount STRING, day STRING, currency STRING").csv(folder).collect()
except Exception as e:
    m = re.search(r"SparkIllegalArgumentException: (.*)\n\s*(.*)", str(e))
    f = re.search(r"reading file \S*/(sales_[\d-]+\.csv)", str(e))
    print("enforceSchema=false ->", f.group(1) if f else "", "\n   ", m.group(1), "\n   ", m.group(2))

section("fix 1: each file's own header, then unionByName")
files = [r.path for r in spark.read.format("binaryFile").load(folder).select("path").collect()]
parts = []
for p in sorted(files):
    names = spark.read.text(p).first().value.split(",")
    schema = ", ".join(f"`{n}` STRING" for n in names) + ", _corrupt_record STRING"
    parts.append(spark.read.schema(schema).option("header", True).csv(p)
                 .withColumn("file", F.col("_metadata.file_name")))
by_name = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)
typed = by_name.select(F.col("id").try_cast("int").alias("id"),
                       F.col("amount").try_cast("decimal(10,2)").alias("amount"),
                       F.col("day").try_cast("date").alias("day"), "currency", "_corrupt_record", "file")
show(typed.orderBy("id"))

section("fix 2: singleVariantColumn")
v = spark.read.option("header", True).option("singleVariantColumn", "v").csv(folder)
show(v.select("v", F.col("_metadata.file_name").alias("file")).orderBy("file").limit(6))
show(v.select(F.try_variant_get("v", "$.id", "int").alias("id"),
              F.try_variant_get("v", "$.amount", "decimal(10,2)").alias("amount"),
              F.try_variant_get("v", "$.day", "date").alias("day"),
              F.try_variant_get("v", "$.currency", "string").alias("currency"))
     .orderBy("id").limit(9))
try:
    spark.read.option("header", True).option("singleVariantColumn", "v").schema("v VARIANT, n INT").csv(folder).collect()
except Exception as e:
    print("schema with another column ->", cond(e))
vb = (spark.read.option("header", True).option("singleVariantColumn", "v")
      .schema("v VARIANT, _corrupt_record STRING").csv(folder).cache())
show(vb.where(F.col("_corrupt_record").isNotNull()))
sku = spark.read.option("header", True).option("singleVariantColumn", "v").csv(str(D / "products.csv"))
print("products.csv sku 00042 inside the variant:", sku.select(F.to_json("v")).first()[0][:80])

section("new in 4.3: the same feed, zipped")
with zipfile.ZipFile(tmp / "sales.zip", "w", zipfile.ZIP_DEFLATED) as z:
    for f in sorted(Path(folder).glob("*.csv")):
        z.write(f, f.name)
spark.conf.set("spark.sql.files.archive.reader.enabled", "true")
zipped = spark.read.option("header", True).csv(str(tmp / "sales.zip"))
print("columns:", zipped.columns, " rows:", zipped.count())
zv = spark.read.option("header", True).option("singleVariantColumn", "v").csv(str(tmp / "sales.zip"))
print("singleVariantColumn from the zip:",
      [r[0] for r in zv.select(F.to_json("v")).orderBy(F.to_json("v")).limit(3).collect()])
spark.conf.unset("spark.sql.files.archive.reader.enabled")

section("CSV inside a column: from_csv and schema_of_csv")
events = spark.createDataFrame([("evt1", "42;19.99;2026-10-01"), ("evt2", "7;abc;2026-10-02"),
                                ("evt3", None), ("evt4", "1;2")], "id STRING, payload STRING")
print("schema_of_csv:", spark.range(1).select(F.schema_of_csv(F.lit("42;19.99;2026-10-01"), {"sep": ";"})).first()[0])
show(events.select("id", F.from_csv("payload", "qty INT, price DOUBLE, day DATE, _corrupt_record STRING",
                                    {"sep": ";"}).alias("p")).select("id", "p.*"))
for label, fn in (("schema_of_csv(column)", lambda: events.select(F.schema_of_csv(F.col("payload"))).collect()),
                  ("from_csv DROPMALFORMED", lambda: events.select(F.from_csv("payload", "qty INT",
                                                                              {"mode": "DROPMALFORMED"})).collect())):
    try:
        fn()
    except Exception as e:
        print(f"{label:24} ->", cond(e))
print("to_csv:", events.select(F.to_csv(F.struct("id", "payload"), {"sep": "|"})).first()[0])

section("fixed width")
lines = spark.read.text(str(D / "stores_fixed.txt"))
recs = lines.where(~F.col("value").startswith("HDR") & ~F.col("value").startswith("TRL"))
stores = recs.select(
    F.substring("value", 1, 4).alias("store_id"),
    F.trim(F.substring("value", 5, 20)).alias("name"),
    F.trim(F.substring("value", 25, 12)).alias("city"),
    F.try_to_date(F.substring("value", 37, 8), "yyyyMMdd").alias("opened"),
    (F.substring("value", 45, 10).try_cast("decimal(12,0)") / 100).cast("decimal(12,2)").alias("sales"),
    F.length("value").alias("chars"),
    F.octet_length("value").alias("bytes"))
show(stores)
trailer = int(lines.where(F.col("value").startswith("TRL")).first().value[3:])
ok = stores.where(F.col("chars") == 54)
print(f"trailer says {trailer}, full-width records: {ok.count()}, quarantined: {stores.count() - ok.count()}")

section("writing CSV: the writer's defaults")
df = spark.createDataFrame([(1, "  padded  ", 'say "hi"', "a,b", "", None, "two\nlines")],
                           "id INT, pad STRING, q STRING, comma STRING, empty STRING, nul STRING, nl STRING")
out = tmp / "out_default"
df.coalesce(1).write.option("header", True).csv(str(out))
print(sorted(re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "<uuid>", p.name) for p in out.iterdir()))
part = glob.glob(str(out / "part-*.csv"))[0]
print(open(part).read(), end="")
print("python csv reads row 1 as:", list(csv.reader(open(part)))[1])
rfc = tmp / "out_rfc"
(df.coalesce(1).write.option("header", True).option("escape", '"')
 .option("ignoreLeadingWhiteSpace", False).option("ignoreTrailingWhiteSpace", False)
 .option("nullValue", "NULL").csv(str(rfc)))
part = glob.glob(str(rfc / "part-*.csv"))[0]
print(open(part).read(), end="")
print("python csv reads row 1 as:", list(csv.reader(open(part)))[1])
back = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
        .option("nullValue", "NULL").schema(df.schema).csv(str(rfc)))
print("round trip equal:", back.collect() == df.collect())
try:
    spark.range(1).select(F.struct(F.lit(1).alias("a")).alias("s")).write.csv(str(tmp / "nested"))
except Exception as e:
    print("struct column ->", cond(e))
