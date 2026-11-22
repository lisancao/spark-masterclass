"""Chapter 15, part 2 (new in Spark 4.3): edit VARIANT values in place. variant_set, variant_insert,
variant_delete, variant_array_append, variant_strip_nulls, their try_ forms, and is_valid_variant.

Run from code/:  uv run python ch15/edit.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, plan, MESSY

spark = session("ch15-edit", spark_sql_session_timeZone="UTC")
EVENTS = str(MESSY / "events")


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_.]+)\]", str(e))
    return m.group(1) if m else f"{type(e).__name__}: {str(e).splitlines()[0][:60]}"


def attempt(label, fn):
    try:
        r = fn()
        print(f"  {label:52} -> {r}")
    except Exception as e:
        print(f"  {label:52} -> {condition(e)}")


def q(sql):
    return lambda: spark.sql(sql).first()[0]


section("the five edit functions on one value")
J = """parse_json('{"user": {"id": 7, "geo": {"country": "CA"}}, "amount": "12.50", "tags": ["a"], "note": null}')"""
for label, sql in [
    ("variant_set(v, '$.amount', 12.5)", f"SELECT variant_set({J}, '$.amount', 12.5)"),
    ("variant_set(v, '$.user.plan', 'pro')", f"SELECT variant_set({J}, '$.user.plan', 'pro')"),
    ("variant_set(v, '$.user.plan', 'pro', false)", f"SELECT variant_set({J}, '$.user.plan', 'pro', false)"),
    ("variant_insert(v, '$.user.plan', 'pro')", f"SELECT variant_insert({J}, '$.user.plan', 'pro')"),
    ("variant_insert(v, '$.amount', 1)", f"SELECT variant_insert({J}, '$.amount', 1)"),
    ("try_variant_insert(v, '$.amount', 1)", f"SELECT try_variant_insert({J}, '$.amount', 1)"),
    ("variant_insert(v, '$.tags[0]', 'z')", f"SELECT variant_insert({J}, '$.tags[0]', 'z')"),
    ("variant_delete(v, '$.user.geo', '$.note')", f"SELECT variant_delete({J}, '$.user.geo', '$.note')"),
    ("variant_delete(v, '$.nothing')", f"SELECT variant_delete({J}, '$.nothing')"),
    ("variant_array_append(v, '$.tags', 'b')", f"SELECT variant_array_append({J}, '$.tags', 'b')"),
    ("variant_array_append(v, '$.amount', 'b')", f"SELECT variant_array_append({J}, '$.amount', 'b')"),
    ("try_variant_array_append(v, '$.amount', 'b')", f"SELECT try_variant_array_append({J}, '$.amount', 'b')"),
    ("variant_strip_nulls(v)", f"SELECT variant_strip_nulls({J})"),
    ("variant_set(v, '$.amount.value', 1)", f"SELECT variant_set({J}, '$.amount.value', 1)"),
    ("try_variant_set(v, '$.amount.value', 1)", f"SELECT try_variant_set({J}, '$.amount.value', 1)"),
    ("variant_set(v, '$', 1)  (the root)", f"SELECT variant_set({J}, '$', 1)"),
    ("variant_set(v, '$.x', NULL)", f"SELECT variant_set({J}, '$.x', NULL)"),
    ("variant_set(v, '$.x', parse_json('null'))", f"SELECT variant_set({J}, '$.x', parse_json('null'))"),
    ("variant_set(v, '$.x', named_struct('a', 1))", f"SELECT variant_set({J}, '$.x', named_struct('a', 1))"),
    ("variant_set(v, '$.tags[3]', 'd')", f"SELECT variant_set({J}, '$.tags[3]', 'd')"),
]:
    attempt(label, q(sql))
attempt("variant_set with a path from a column",
        q(f"SELECT variant_set({J}, p, 1) FROM VALUES ('$.a'), ('$.b') AS t(p)"))
attempt("variant_set with a column create_if_missing",
        q(f"SELECT variant_set({J}, '$.a', 1, c) FROM VALUES (true) AS t(c)"))
attempt("variant_strip_nulls(v, false)  (keep array NULLs)",
        q("""SELECT variant_strip_nulls(parse_json('{"a": [1, null, {"b": null}]}'), false)"""))
attempt("variant_strip_nulls(v)  (default)",
        q("""SELECT variant_strip_nulls(parse_json('{"a": [1, null, {"b": null}]}'))"""))

section("is_valid_variant (4.2): check bytes that came from outside Spark")
attempt("is_valid_variant(parse_json('[1]'))", q("SELECT is_valid_variant(parse_json('[1]'))"))

section("fix the events in place: one shape for amount, no geo, a schema version")
ev = (spark.read.option("singleVariantColumn", "v").json(EVENTS)
      .where("v IS NOT NULL").withColumn("file", F.col("_metadata.file_name")))
amount = F.coalesce(F.try_variant_get("v", "$.amount", "decimal(10,2)"),
                    F.try_variant_get("v", "$.amount.value", "decimal(10,2)"))
user_id = F.coalesce(F.try_variant_get("v", "$.user.id", "bigint"), F.try_variant_get("v", "$.user_id", "bigint"))
event = F.coalesce(F.try_variant_get("v", "$.event_type", "string"), F.try_variant_get("v", "$.event", "string"))
v = F.col("v")
v = F.variant_set(v, F.lit("$.amount"), amount)          # one shape: a decimal number
v = F.variant_set(v, F.lit("$.user.id"), user_id)        # move user_id into user
v = F.variant_set(v, F.lit("$.event_type"), event)       # one name for the event
v = F.variant_set(v, F.lit("$.v"), F.lit(2))             # a schema version
v = F.variant_delete(v, F.lit("$.user.geo"), F.lit("$.user_id"), F.lit("$.event"))
fixed = ev.select("file", v.alias("v"))
fixed.createOrReplaceTempView("fixed")
show(spark.sql("""SELECT file, schema_of_variant_agg(v) AS schema FROM fixed GROUP BY file ORDER BY file"""))
show(fixed.select("file", F.to_json("v").alias("v")).orderBy("file").limit(3).union(
     fixed.select("file", F.to_json("v")).where("file LIKE '%10-03%'").limit(1)))
print("sum of amounts after the edit:",
      fixed.select(F.sum(F.try_variant_get("v", "$.amount", "decimal(10,2)"))).first()[0])
print("plan of the edit:")
print("  " + plan(fixed, "simple").splitlines()[1][:160] + " ...")

