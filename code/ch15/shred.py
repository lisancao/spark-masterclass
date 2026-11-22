"""Chapter 15, part 3: VARIANT in Parquet. The unshredded and shredded layouts read from the footer,
how the writer picks a shredding schema for each file, and which extractions the scan can push down.

Run from code/:  uv run python ch15/shred.py
"""
import glob
import re
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow.parquet as pq
from pyspark.sql import functions as F
from common.book import session, show, plan, MESSY, WORK

spark = session("ch15-shred", spark_sql_session_timeZone="UTC")
EVENTS = str(MESSY / "events")
OUT = WORK / "ch15"
shutil.rmtree(OUT, ignore_errors=True)


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_.]+)\]", str(e))
    return m.group(1) if m else f"{type(e).__name__}: {str(e).splitlines()[0][:60]}"


def write_one(df, name):
    path = str(OUT / name)
    df.coalesce(1).write.mode("overwrite").parquet(path)
    return path, glob.glob(f"{path}/*.parquet")[0]


def leaves(f):
    s = pq.ParquetFile(f).schema
    return [s.column(i).path for i in range(len(s))]


def shredded(cols):
    """'v.typed_value.user.typed_value.id.typed_value' -> 'user.id'; skip the binary fallbacks."""
    out = set()
    for c in cols:
        parts = c.split(".")
        if parts[-1] != "typed_value":
            continue
        names = []
        for p in parts[1:-1]:
            if p in ("typed_value", "list"):
                continue
            if p == "element":
                names[-1] += "[]"
            else:
                names.append(p)
        out.add(".".join(names) if names else "(top level)")
    return sorted(out)


ev = spark.read.option("singleVariantColumn", "v").json(EVENTS)

section("shredding off: two binary columns")
spark.conf.set("spark.sql.variant.writeShredding.enabled", "false")
_, f = write_one(ev, "plain")
spark.conf.unset("spark.sql.variant.writeShredding.enabled")
print("\n".join(str(pq.ParquetFile(f).schema).splitlines()[1:]))

section("shredding on (the default): the footer of the three-day file")
all_path, f = write_one(ev, "all")
cols = leaves(f)
print(len(cols), "leaf columns, for example:")
for c in cols:
    if c.startswith("v.typed_value.user") or c in ("v.metadata", "v.value") or c.startswith("v.typed_value.amount"):
        print("  " + c)
print("shredded fields:", shredded(cols))
print("Parquet logical type:", pq.ParquetFile(f).schema.column(0).logical_type if False else str(pq.ParquetFile(f).schema).splitlines()[2].strip())

section("one file for each day: each file gets its own shredding schema")
for day in ("10-01", "10-02", "10-03"):
    _, fd = write_one(ev.where(F.col("_metadata.file_name").contains(day)), f"day_{day}")
    print(f"  {day}: {shredded(leaves(fd))}")

section("a field in fewer than 10% of the rows is not shredded")
rare = spark.range(1000).select(F.when(F.col("id") < 90, F.parse_json(F.lit('{"a": 1, "rare": 2}')))
                                .otherwise(F.parse_json(F.lit('{"a": 1}'))).alias("v"))
_, fr = write_one(rare, "rare_9pct")
print("  rare in 9% of rows: ", shredded(leaves(fr)))
rare = spark.range(1000).select(F.when(F.col("id") < 110, F.parse_json(F.lit('{"a": 1, "rare": 2}')))
                                .otherwise(F.parse_json(F.lit('{"a": 1}'))).alias("v"))
_, fr = write_one(rare, "rare_11pct")
print("  rare in 11% of rows:", shredded(leaves(fr)))
mixed = spark.range(1000).select(F.when(F.col("id") % 2 == 0, F.parse_json(F.lit('{"a": 1}')))
                                 .otherwise(F.parse_json(F.lit('{"a": "x"}'))).alias("v"))
_, fm = write_one(mixed, "mixed")
print("  a: half BIGINT, half STRING:", shredded(leaves(fm)))
late = spark.range(10000).select(F.when(F.col("id") < 5000, F.parse_json(F.lit('{"a": 1}')))
                                 .otherwise(F.parse_json(F.lit('{"a": 1, "late": 2}'))).alias("v"))
_, fl = write_one(late, "late")
print("  late appears after row 5000:", shredded(leaves(fl)))

section("what the scan reads: try_variant_get against a path and a cast")
stored = spark.read.parquet(all_path)


def scan_line(df, key):
    for l in plan(df, "formatted").splitlines():
        if l.strip().startswith(key):
            return l.split(": ", 1)[1]
    return "-"


for label, df in [
    ("select v", stored.select("v")),
    ("try_variant_get(v, '$.user.plan', 'string')", stored.select(F.try_variant_get("v", "$.user.plan", "string"))),
    ("v:user.plan::string", stored.selectExpr("v:user.plan::string")),
    ("variant_get(v, '$.user.plan', 'string')", stored.select(F.variant_get("v", "$.user.plan", "string"))),
    ("v:user.plan  (a VARIANT)", stored.selectExpr("v:user.plan")),
]:
    print(f"  {label:46} ReadSchema: {scan_line(df, 'ReadSchema')[:60]}")
print()
for label, df in [
    ("where try_variant_get(...) = 'pro'", stored.where(F.try_variant_get("v", "$.user.plan", "string") == "pro")),
    ("where v:user.plan::string = 'pro'", stored.where("v:user.plan::string = 'pro'")),
]:
    print(f"  {label:38} PushedFilters: {scan_line(df, 'PushedFilters')[:70]}")

section("new in 4.3: extractions inside an aggregate are pushed into the scan too")
agg = stored.groupBy(F.try_variant_get("v", "$.user.plan", "string").alias("plan")).count()
show(agg.orderBy("plan"))
summ = lambda: stored.agg(F.sum(F.try_variant_get("v", "$.amount", "double")).alias("amount"),
                          F.count_if(F.try_variant_get("v", "$.user.plan", "string") == "pro").alias("pro"))
print("  sum(...), count_if(...): pullOutExtractions=true  ReadSchema:", scan_line(summ(), "ReadSchema")[:60])
spark.conf.set("spark.sql.variant.pushVariantIntoScan.pullOutExtractions", "false")
print("  sum(...), count_if(...): pullOutExtractions=false ReadSchema:", scan_line(summ(), "ReadSchema")[:60])
spark.conf.unset("spark.sql.variant.pushVariantIntoScan.pullOutExtractions")
print("  result:", summ().first())

section("a strict cast that fails inside the scan")
try:
    stored.select(F.variant_get("v", "$.amount", "double")).collect()
    print("  no error")
except Exception as e:
    msg = str(e)
    print("  exception:", type(e).__name__)
    print("  conditions in the message:", sorted(set(re.findall(r"\[([A-Z_.]+)\]", msg))))
