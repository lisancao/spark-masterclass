"""Chapter 13, part 3: JSON inside a column. from_json, schema_of_json, get_json_object, json_tuple,
new in 4.3: JSON_TABLE and the explodeEmbeddedArray reader option, and the JSON writer.

Run from code/:  uv run python ch13/functions.py
"""
import json
import re
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, plan, MESSY, WORK

spark = session("ch13-functions", spark_sql_session_timeZone="UTC")
work = WORK / "ch13" / "functions"
shutil.rmtree(work, ignore_errors=True)
work.mkdir(parents=True)


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_.]+)\]", str(e))
    return m.group(1) if m else type(e).__name__


log = spark.read.text(str(MESSY / "logs" / "app.log"))
js = log.where(F.col("value").startswith("{")).withColumnRenamed("value", "line").cache()
print("app.log lines:", log.count(), " JSON lines:", js.count())
print("first:", js.first().line)

section("schema_of_json needs a literal sample")
sample = js.first().line
ddl = spark.range(1).select(F.schema_of_json(F.lit(sample))).first()[0]
print("schema_of_json(lit(sample)):", ddl)
try:
    spark.range(1).select(F.schema_of_json("processed")).first()
except Exception as e:
    msg = str(e).splitlines()
    print("schema_of_json('processed'): the string is the JSON text, not a column name:")
    print("  ", next((l.strip() for l in msg if "Unrecognized token" in l), msg[0])[:88])
try:
    js.select(F.schema_of_json(F.col("line"))).first()
except Exception as e:
    print("schema_of_json(col('line')):", condition(e))
print("one sample hides the keys it does not have:",
      spark.range(1).select(F.schema_of_json(F.lit('{"a": 1}'))).first()[0], "for", '{"a": 1, "b": 2} rows too')

section("from_json with the sampled schema")
parsed = js.select(F.from_json("line", ddl).alias("j")).select("j.*")
parsed.printSchema()
show(parsed, 3)

section("from_json on bad text: a struct of NULLs, or the corrupt column")
bad = spark.createDataFrame([('{"order": 1, "ms": 20}',), ('{"order": 2, "ms": "slow"}',), ('{"order": 3',)], "line STRING")
show(bad.select("line", F.from_json("line", "order INT, ms INT").alias("j")))
show(bad.select(F.from_json("line", "order INT, ms INT, _corrupt_record STRING").alias("j")).select("j.*"))
try:
    bad.select(F.from_json("line", "order INT", {"mode": "DROPMALFORMED"})).collect()
except Exception as e:
    print("mode=DROPMALFORMED:", condition(e))
try:
    bad.select(F.from_json("line", "order INT", {"mode": "FAILFAST"})).collect()
except Exception as e:
    print("mode=FAILFAST:", condition(e))

section("fixed in 4.3 (SPARK-58707): select only the corrupt field of from_json")
only = bad.select(F.from_json("line", "order INT, ms INT, _corrupt_record STRING").alias("j")) \
          .select("j._corrupt_record")
show(only)

section("no schema at all: get_json_object and json_tuple")
show(js.select(F.get_json_object("line", "$.level").alias("level"),
               F.get_json_object("line", "$.order").alias("order"),
               F.get_json_object("line", "$.missing").alias("missing")), 2)
show(js.select(F.json_tuple("line", "level", "order", "ms")), 2)
show(spark.createDataFrame([('{"a": null}',), ('{}',)], "line STRING")
     .select("line", F.get_json_object("line", "$.a").alias("get_json_object $.a")))

section("new in 4.3: JSON_TABLE")
js.createOrReplaceTempView("app_json")
jt = spark.sql("""
    SELECT t.*
    FROM app_json, LATERAL
         JSON_TABLE(line, '$'
                    COLUMNS (ts TIMESTAMP PATH '$.ts',
                             level STRING PATH '$.level',
                             order_id BIGINT PATH '$.order',
                             ms INT PATH '$.ms',
                             has_error BOOLEAN EXISTS PATH '$.error')) AS t
""")
jt.printSchema()
show(jt, 3)
print("rows:", jt.count())

raw = spark.read.text(str(MESSY / "events")).withColumnRenamed("value", "line")
raw.createOrReplaceTempView("event_lines")
items = spark.sql("""
    SELECT get_json_object(e.line, '$.ts') AS ts, i.*
    FROM event_lines e, LATERAL
         JSON_TABLE(e.line, '$.items[*]'
                    COLUMNS (n FOR ORDINALITY,
                             sku STRING PATH '$.sku',
                             qty INT PATH '$.qty')) AS i
""")
show(items, 5)
print("item rows:", items.count())
print("item rows from inline(items) in drift.py: 61")

section("JSON_TABLE: missing, JSON null and bad text")
show(spark.sql("""
    SELECT doc, t.*
    FROM VALUES ('{"a": 1}'), ('{"a": null}'), ('{}'), ('{"a": ') AS v(doc), LATERAL
         JSON_TABLE(doc, '$' COLUMNS (a INT PATH '$.a', a_exists BOOLEAN EXISTS PATH '$.a')) AS t
"""))
try:
    spark.sql("""SELECT * FROM JSON_TABLE('{"a": "x"}', '$' COLUMNS (a INT PATH '$.a')) AS t""").collect()
except Exception as e:
    print("a value that does not cast, with the default NULL ON ERROR:", condition(e))
try:
    spark.sql("""SELECT * FROM JSON_TABLE('{"a": ', '$' COLUMNS (a INT PATH '$.a') ERROR ON ERROR) AS t""").collect()
except Exception as e:
    print("ERROR ON ERROR on bad text:", condition(e))
try:
    spark.sql("""SELECT * FROM JSON_TABLE('{"a": [1]}', '$.a[*].b[*]' COLUMNS (x INT PATH '$')) AS t""").collect()
except Exception as e:
    print("a wildcard that is not at the end of the row path:", condition(e))

section("JSON_TABLE in the plan")
p = plan(items, "formatted")
print(p.split("\n\n")[0])
args = next(l for l in p.splitlines() if l.startswith("Arguments: json_table"))
print("(3) Generate", args[:88])

section("new in 4.3: explodeEmbeddedArray reads {'Records': [...]} one element at a time")
trail = work / "cloudtrail"
trail.mkdir()
for day in (1, 2):
    doc = {"Records": [{"eventName": n, "awsRegion": "us-east-1", "requestParameters": {"bucket": "logs"}}
                       for n in (["GetObject", "PutObject"] if day == 1 else ["DeleteObject"])]}
    (trail / f"day{day}.json").write_text(json.dumps(doc, indent=2) + "\n")
plain = spark.read.option("multiLine", True).json(str(trail))
print("multiLine, no option: rows", plain.count(), " columns", plain.columns)
rec = spark.read.option("explodeEmbeddedArray", "Records").json(str(trail))
rec.printSchema()
print("rows:", rec.count())
show(rec.select(F.expr("Records:eventName::string").alias("eventName"),
                F.expr("Records:requestParameters.bucket::string").alias("bucket")))
try:
    spark.read.option("explodeEmbeddedArray", "Records").option("singleVariantColumn", "v").json(str(trail)).count()
except Exception as e:
    print("with singleVariantColumn too:", condition(e))

section("writing JSON: NULL fields, timestamps, to_json")
silver = parsed.select("ts", "level", "order", "ms").limit(2).withColumn("note", F.lit(None).cast("string"))
silver.write.mode("overwrite").json(str(work / "out_default"))
silver.write.mode("overwrite").option("ignoreNullFields", False).json(str(work / "out_nulls"))
print("default:              ", next((work / "out_default").glob("part-*")).read_text().splitlines()[0])
print("ignoreNullFields=false:", next((work / "out_nulls").glob("part-*")).read_text().splitlines()[0])
show(silver.select(F.to_json(F.struct("order", "ms", "note")).alias("to_json")), 1)
