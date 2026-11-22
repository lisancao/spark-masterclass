"""Chapter 13, part 1: how the JSON reader finds records and infers a schema.
JSON Lines and multiLine, the inference pass, the type merge rules, and the reader options.

Run from code/:  uv run python ch13/infer.py
"""
import json
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, ui_api, MESSY, WORK

spark = session("ch13-infer", spark_sql_session_timeZone="UTC")
EVENTS = str(MESSY / "events")
work = WORK / "ch13" / "infer"
shutil.rmtree(work, ignore_errors=True)
work.mkdir(parents=True)


def section(title):
    print(f"\n== {title} ==")


def write(name, text):
    p = work / name
    p.write_text(text)
    return str(p)


def jobs_for(label, fn):
    """Run fn under a job group and count the jobs it started, from the REST API."""
    spark.sparkContext.setJobGroup(label, label)
    fn()
    spark.sparkContext.setJobGroup("", "")
    return len([j for j in ui_api("jobs") if j.get("jobGroup") == label])


section("JSON Lines: one record per line")
lines = write("lines.json", '{"id": 1, "name": "a"}\n{"id": 2, "name": "b"}\n')
show(spark.read.json(lines))

section("a pretty-printed array without multiLine, and with it")
pretty = write("pretty.json", json.dumps([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], indent=2) + "\n")
print(open(pretty).read().rstrip())
bad = spark.read.json(pretty)
print("columns:", bad.columns)
try:
    bad.cache().count()
except Exception as e:
    print("cache().count():", str(e).split("]")[0] + "]")
show(spark.read.option("multiLine", True).json(pretty))
big = work / "big_lines"
spark.range(200_000).selectExpr("id", "uuid() AS name").write.json(str(big))
print("JSON Lines folder, scan tasks:", spark.read.schema("id BIGINT, name STRING").json(str(big)).rdd.getNumPartitions())
one_file = str(work / "one_array.json")
with open(one_file, "w") as f:
    f.write("[\n" + ",\n".join(json.dumps({"id": i, "name": f"n{i}"}) for i in range(200_000)) + "\n]\n")
print(f"one {Path(one_file).stat().st_size / 1e6:.1f} MB array file, multiLine, scan tasks:",
      spark.read.schema("id BIGINT, name STRING").option("multiLine", True).json(one_file).rdd.getNumPartitions())

section("inference is a job of its own")
print("read.json(events), no schema:      ",
      jobs_for("infer", lambda: spark.read.json(EVENTS)), "job(s) before any action")
print("read.schema(ddl).json(events):     ",
      jobs_for("ddl", lambda: spark.read.schema("event STRING").json(EVENTS)), "job(s)")
print("samplingRatio=0.1:                 ",
      jobs_for("sample", lambda: spark.read.option("samplingRatio", 0.1).json(EVENTS)), "job(s)")

section("the schema that inference gives the three days")
events = spark.read.json(EVENTS)
events.printSchema()

section("how inference merges two values of one field")
cases = {
    "int + int": ['{"v": 1}', '{"v": 2}'],
    "int + double": ['{"v": 1}', '{"v": 2.5}'],
    "int + big int": ['{"v": 1}', '{"v": 99999999999999999999}'],
    "number + string": ['{"v": 1}', '{"v": "2"}'],
    "number + bool": ['{"v": 1}', '{"v": true}'],
    "number + object": ['{"v": 1}', '{"v": {"x": 1}}'],
    "object + object": ['{"v": {"x": 1}}', '{"v": {"y": "a"}}'],
    "array + scalar": ['{"v": [1, 2]}', '{"v": 3}'],
    "null + int": ['{"v": null}', '{"v": 1}'],
    "only null": ['{"v": null}', '{"v": null}'],
    "empty array": ['{"v": []}', '{"v": []}'],
    "ISO time": ['{"v": "2026-10-01T10:00:00"}'],
    "ISO date": ['{"v": "2026-10-01"}'],
}
for label, recs in cases.items():
    p = write("case.json", "\n".join(recs) + "\n")
    dt = spark.read.json(p).schema["v"].dataType.simpleString()
    print(f"  {label:18} -> {dt}")

section("options that change inference")
p = write("opts.json", '{"v": 1.5, "t": "2026-10-01T10:00:00", "n": null, "i": 7}\n')
for opts in ({}, {"prefersDecimal": True}, {"primitivesAsString": True},
             {"inferTimestamp": True}, {"dropFieldIfAllNull": True}):
    s = spark.read.options(**opts).json(p).schema.simpleString()
    print(f"  {str(opts) if opts else 'defaults':30} {s}")

section("options that make the parser accept non-standard JSON")
samples = {
    "allowComments": '/* note */ {"v": 1}',
    "allowUnquotedFieldNames": '{v: 1}',
    "allowSingleQuotes": "{'v': 1}",
    "allowNumericLeadingZeros": '{"v": 007}',
    "allowNonNumericNumbers": '{"v": NaN}',
    "allowBackslashEscapingAnyCharacter": '{"v": "a\\$b"}',
}
for opt, text in samples.items():
    p = write("opt.json", text + "\n")
    res = []
    for val in ("false", "true"):
        df = spark.read.schema("v STRING, _corrupt_record STRING").option(opt, val).json(p)
        r = df.collect()[0]
        res.append("corrupt" if r["_corrupt_record"] else f"v={r['v']}")
    default = spark.read.schema("v STRING, _corrupt_record STRING").json(p).collect()[0]
    print(f"  {opt:35} {text:22} false: {res[0]:8} true: {res[1]:8} default: "
          f"{'corrupt' if default['_corrupt_record'] else 'v=' + str(default['v'])}")
