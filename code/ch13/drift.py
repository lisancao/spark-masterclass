"""Chapter 13, part 2: parse modes, the corrupt record column, and a quarantine for drifting JSON.

Run from code/:  uv run python ch13/drift.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, plan, MESSY

spark = session("ch13-drift", spark_sql_session_timeZone="UTC")
EVENTS = str(MESSY / "events")


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_.]+)\]", str(e))
    return m.group(1) if m else type(e).__name__


EVENT_SCHEMA = """
    event STRING,
    event_type STRING,
    user_id BIGINT,
    user STRUCT<id: BIGINT, plan: STRING, geo: STRUCT<country: STRING>>,
    amount STRING,
    items ARRAY<STRUCT<sku: STRING, qty: INT>>,
    ts TIMESTAMP,
    _corrupt STRING
"""

section("the three modes, with inference")
for mode in ("PERMISSIVE", "DROPMALFORMED", "FAILFAST"):
    try:
        df = spark.read.option("mode", mode).json(EVENTS)
        print(f"  {mode:13} schema ok, rows: {df.count()}")
    except Exception as e:
        print(f"  {mode:13} fails at read time: {condition(e)}")

section("the three modes, with an explicit schema")
for mode in ("PERMISSIVE", "DROPMALFORMED", "FAILFAST"):
    df = spark.read.schema(EVENT_SCHEMA).option("mode", mode) \
        .option("columnNameOfCorruptRecord", "_corrupt").json(EVENTS)
    print(f"  {mode:13} read() returns a DataFrame;", end=" ")
    try:
        print(f"rows: {df.count()}")
    except Exception as e:
        print(f"the action fails: {condition(e)}")

section("query only the corrupt column")
raw = spark.read.schema(EVENT_SCHEMA).option("columnNameOfCorruptRecord", "_corrupt").json(EVENTS)
try:
    raw.where(F.col("_corrupt").isNotNull()).select("_corrupt").count()
except Exception as e:
    print("without cache:", condition(e))
raw = raw.withColumn("file", F.col("_metadata.file_name")).cache()
print("cached, with a data column next to it:", raw.where(F.col("_corrupt").isNotNull()).count(), "corrupt")
show(raw.where(F.col("_corrupt").isNotNull()).select("file", "_corrupt"))

section("the corrupt column must be a nullable STRING")
try:
    spark.read.schema("event STRING, _corrupt_record INT").json(EVENTS).count()
except Exception as e:
    print("_corrupt_record INT:", condition(e))

section("a strict schema marks a whole record, and keeps the fields that parsed")
strict = (spark.read.schema("event STRING, user_id BIGINT, amount DOUBLE, ts TIMESTAMP, _corrupt_record STRING")
          .json(str(MESSY / "events" / "events_2026-10-02.json")).cache())
print("rows:", strict.count(), " with _corrupt_record:", strict.where("_corrupt_record IS NOT NULL").count())
show(strict.where("_corrupt_record IS NOT NULL").select(
    "event", "amount", "ts", F.substring("_corrupt_record", 1, 48).alias("_corrupt_record (first 48)")), 3)

section("absorb the drift: STRING for the field that changes, then coalesce")
amount = F.coalesce(
    F.col("amount").try_cast("double"),                           # 12.5 or "12.5"
    F.get_json_object("amount", "$.value").try_cast("double"),   # {"value": 12.5, ...}
)
silver = (raw.where(F.col("_corrupt").isNull())
          .select(F.coalesce("event_type", "event").alias("event_type"),
                  F.coalesce(F.col("user.id"), F.col("user_id")).alias("user_id"),
                  F.col("user.plan").alias("plan"),
                  F.col("user.geo.country").alias("country"),
                  amount.alias("amount"), "ts", "items", "file"))
quarantine = raw.where(F.col("_corrupt").isNotNull()).select("file", "_corrupt")
show(silver.where("file = 'events_2026-10-01.json'").drop("items"), 2)
show(silver.where("file = 'events_2026-10-03.json'").drop("items"), 2)
print("amount shapes in raw text:")
show(raw.where(F.col("_corrupt").isNull()).groupBy(
    F.when(F.col("amount").startswith("{"), "object")
     .when(F.col("amount").rlike(r"^-?[0-9.]+$"), "number or numeric string")
     .otherwise("other").alias("shape"), "file").count().orderBy("file", "shape"))

section("the books must balance, file by file")
books = (raw.groupBy("file").agg(F.count("*").alias("read"))
         .join(silver.groupBy("file").agg(F.count("*").alias("silver")), "file", "left")
         .join(quarantine.groupBy("file").agg(F.count("*").alias("quarantine")), "file", "left")
         .fillna(0).orderBy("file"))
show(books)
print("read == silver + quarantine:", books.where("read != silver + quarantine").count() == 0)
print("sum(amount):", silver.agg(F.round(F.sum("amount"), 2)).first()[0],
      " NULL amounts:", silver.where("amount IS NULL").count())

section("arrays to rows: explode, explode_outer, inline, inline_outer")
for name, gen in (("explode", F.explode), ("explode_outer", F.explode_outer),
                  ("inline", F.inline), ("inline_outer", F.inline_outer)):
    arg = F.col("items")
    df = silver.select("user_id", gen(arg))
    print(f"  {name:14} rows: {df.count():3}  columns: {df.columns}")
items = silver.select("ts", "user_id", F.inline_outer("items"))
show(items.where("sku IS NOT NULL").groupBy("sku").agg(F.sum("qty").alias("qty")).orderBy("sku"))

section("the plan of explode: the optimizer adds a filter")
opt = plan(silver.select("user_id", F.explode("items")), "extended").split("== Optimized Logical Plan ==")[1]
print("\n".join(opt.strip().splitlines()[:3]))
