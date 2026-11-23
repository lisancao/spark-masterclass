"""Chapter 29, part 1: land the drifting events as VARIANT, read paths, profile keys and types,
normalize the shapes, explode arrays, and the rules of the type.

Run from code/:  uv run python ch29/land.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, plan, MESSY

spark = session("ch29-land", spark_sql_session_timeZone="UTC")
EVENTS = str(MESSY / "events")


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_.]+)\]", str(e))
    return m.group(1) if m else f"{type(e).__name__}: {str(e).splitlines()[0][:60]}"


def attempt(label, fn):
    try:
        r = fn()
        print(f"  {label:44} -> {r}")
    except Exception as e:
        print(f"  {label:44} -> {condition(e)}")


section("parse_json and try_parse_json")
attempt("parse_json('{\"a\": 1}')", lambda: spark.sql("""SELECT parse_json('{"a": 1}')""").first()[0])
attempt("parse_json('{\"a\": ')", lambda: spark.sql("""SELECT parse_json('{"a": ')""").first()[0])
attempt("try_parse_json('{\"a\": ')", lambda: spark.sql("""SELECT try_parse_json('{"a": ')""").first()[0])
spark.conf.set("spark.sql.ansi.enabled", "false")
attempt("parse_json('{\"a\": '), ANSI off", lambda: spark.sql("""SELECT parse_json('{"a": ')""").first()[0])
spark.conf.unset("spark.sql.ansi.enabled")
v = spark.sql("""SELECT parse_json('{"user": {"id": 7, "plan": "pro"}, "amount": "12.50",
                                     "items": [{"sku": "P1"}, {"sku": "P2"}], "note": null,
                                     "odd key": 1}') AS v""")
r = v.first()
print("  collected:", type(r.v).__name__, " toPython():", r.v.toPython())

section("paths and casts")
v.createOrReplaceTempView("one")
for label, sql in [
    ("v:user.id", "SELECT v:user.id FROM one"),
    ("typeof(v:user.id)", "SELECT typeof(v:user.id) FROM one"),
    ("v:user.id::int", "SELECT v:user.id::int FROM one"),
    ("v:items[1].sku::string", "SELECT v:items[1].sku::string FROM one"),
    ("v:['odd key']", "SELECT v:['odd key'] FROM one"),
    ("v:User.id (case)", "SELECT v:User.id FROM one"),
    ("v:missing", "SELECT v:missing FROM one"),
    ("variant_get(v, '$.user.plan', 'string')", "SELECT variant_get(v, '$.user.plan', 'string') FROM one"),
    ("v:amount::double  (a string)", "SELECT v:amount::double FROM one"),
    ("v:user.plan::int", "SELECT v:user.plan::int FROM one"),
    ("try_variant_get(v, '$.user.plan', 'int')", "SELECT try_variant_get(v, '$.user.plan', 'int') FROM one"),
    ("parse_json('12.5')::int", "SELECT parse_json('12.5')::int"),
    ("parse_json('\"12.5\"')::int", "SELECT parse_json('\"12.5\"')::int"),
    ("v:items::array<variant>", "SELECT size(v:items::array<variant>) FROM one"),
    ("v:user::map<string,string>", "SELECT v:user::map<string,string> FROM one"),
]:
    attempt(label, lambda sql=sql: spark.sql(sql).first()[0])

section("JSON null and a missing key")
show(spark.sql("""SELECT v:note IS NULL AS note_is_sql_null, is_variant_null(v:note) AS note_is_json_null,
                         v:missing IS NULL AS missing_is_sql_null, is_variant_null(v:missing) AS missing_is_json_null,
                         try_variant_get(v, '$.note', 'string') AS note_as_string FROM one"""))

section("schema_of_variant and schema_of_variant_agg")
show(spark.sql("SELECT schema_of_variant(v) AS one_row FROM one"))
print(spark.sql("""SELECT schema_of_variant_agg(parse_json(j)) FROM VALUES
                   ('{"a": 1}'), ('{"a": 2.5}'), ('{"a": "x"}'), ('{"b": [1]}') AS t(j)""").first()[0])
print(spark.sql("""SELECT schema_of_variant_agg(parse_json(j)) FROM VALUES
                   ('{"a": 1}'), ('{"a": 123456789012}'), ('{"a": 2.5}') AS t(j)""").first()[0])

section("land the three days as VARIANT")
ev = (spark.read
      .option("singleVariantColumn", "v")
      .option("columnNameOfCorruptRecord", "_bad")
      .schema("v VARIANT, _bad STRING")
      .json(EVENTS)
      .withColumn("file", F.col("_metadata.file_name"))
      .cache())
ev.printSchema()
print("rows:", ev.count(), " malformed:", ev.where("_bad IS NOT NULL").count())
show(ev.where("_bad IS NOT NULL").select("v", F.substring("_bad", 1, 50).alias("_bad"), "file"))
print("only singleVariantColumn, no schema:",
      spark.read.option("singleVariantColumn", "v").json(EVENTS).columns,
      " NULL rows:", spark.read.option("singleVariantColumn", "v").json(EVENTS).where("v IS NULL").count())
attempt("schema() before the options (classic)", lambda: spark.read.schema("v VARIANT, _bad STRING")
        .option("singleVariantColumn", "v").option("columnNameOfCorruptRecord", "_bad")
        .json(EVENTS).count())
print("no inference job: the read gives the schema without a pass over the data")

section("profile: every top-level key, how often, and its types")
ev.createOrReplaceTempView("ev")
show(spark.sql("""
    SELECT key, count(*) AS n,
           sort_array(collect_set(regexp_extract(schema_of_variant(value), '^[A-Z]+', 0))) AS types,
           sort_array(collect_set(schema_of_variant(value))) AS exact
    FROM ev, LATERAL variant_explode(v)
    GROUP BY key ORDER BY key"""))
show(spark.sql("""SELECT file, schema_of_variant_agg(v:amount) AS amount_schema
                  FROM ev WHERE v IS NOT NULL GROUP BY file ORDER BY file"""))

section("variant_explode: what it gives for an object, an array and a scalar")
show(spark.sql("""SELECT 'object' AS input, e.* FROM LATERAL variant_explode(parse_json('{"b": 2, "a": 1}')) e
                  UNION ALL SELECT 'array', e.* FROM LATERAL variant_explode(parse_json('["x", "y"]')) e
                  UNION ALL SELECT 'scalar', e.* FROM LATERAL variant_explode(parse_json('5')) e"""))
print("variant_explode_outer(v:items):", spark.sql("SELECT * FROM ev, LATERAL variant_explode_outer(v:items)").count(),
      " variant_explode(v:items):", spark.sql("SELECT * FROM ev, LATERAL variant_explode(v:items)").count())

section("normalize the three shapes into typed columns")


def get(path, type_):
    return F.try_variant_get("v", path, type_)


silver = (ev.where("_bad IS NULL")
          .select(F.coalesce(get("$.event_type", "string"), get("$.event", "string")).alias("event_type"),
                  F.coalesce(get("$.user.id", "bigint"), get("$.user_id", "bigint")).alias("user_id"),
                  get("$.user.plan", "string").alias("plan"),
                  F.coalesce(get("$.amount", "double"), get("$.amount.value", "double")).alias("amount"),
                  get("$.ts", "timestamp").alias("ts"),
                  "file"))
silver.printSchema()
show(silver.groupBy("file").agg(F.count("*").alias("rows"), F.round(F.sum("amount"), 2).alias("amount"),
                                F.count_if(F.col("user_id").isNull()).alias("null_users"),
                                F.count_if(F.col("amount").isNull()).alias("null_amounts")).orderBy("file"))

items = spark.sql("""SELECT item.value:sku::string AS sku, item.value:qty::int AS qty
                     FROM ev, LATERAL variant_explode_outer(v:items) AS item""")
show(items.where("sku IS NOT NULL").groupBy("sku").agg(F.sum("qty").alias("qty")).orderBy("sku"))
print("items via explode(try_variant_get(v, '$.items', 'array<variant>')):",
      ev.select(F.explode(get("$.items", "array<variant>"))).count())

section("the rules of the type")
attempt("GROUP BY v", lambda: spark.sql("SELECT v, count(*) FROM ev GROUP BY v").collect())
attempt("ORDER BY v", lambda: spark.sql("SELECT v FROM ev ORDER BY v").collect())
attempt("v = v", lambda: spark.sql("SELECT count(*) FROM ev WHERE v = v").collect())
attempt("DISTINCT v", lambda: spark.sql("SELECT DISTINCT v FROM ev").collect())
attempt("GROUP BY to_json(v)", lambda: spark.sql("SELECT count(DISTINCT to_json(v)) FROM ev").first()[0])
print("  to_json sorts keys:", spark.sql("""SELECT to_json(parse_json('{"b": 1, "a": 2}'))""").first()[0])
print("  number types:", [spark.sql(f"SELECT schema_of_variant(parse_json('{n}'))").first()[0]
                          for n in ("12.34", "1e3", "7", "12345678901234567890")])
print("  0.1 + 0.2 as DECIMAL:", spark.sql("SELECT parse_json('0.1')::decimal(10,2) + parse_json('0.2')::decimal(10,2)").first()[0],
      " as DOUBLE:", spark.sql("SELECT parse_json('0.1')::double + parse_json('0.2')::double").first()[0])
print("  to_variant_object(struct):", spark.sql("SELECT to_variant_object(named_struct('a', 1, 'b', array(1, 2)))").first()[0])
attempt("CAST(named_struct('a', 1) AS VARIANT)", lambda: spark.sql("SELECT CAST(named_struct('a', 1) AS VARIANT)").first()[0])
attempt("CAST(map('a', 1) AS VARIANT)", lambda: spark.sql("SELECT CAST(map('a', 1) AS VARIANT)").first()[0])
