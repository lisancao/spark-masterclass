"""Chapter 73: casefile in use. Every table, plan and file the chapter shows.

Run from code/:  uv run python ch73/demo.py
"""
import json, os, shutil, sys, tempfile, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import casefile as cf  # noqa: E402
from common.book import session  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

DATA = str(Path(__file__).resolve().parents[1] / "data" / "messy" / "casefiles")
TMP = Path(tempfile.gettempdir())


def head(t):
    print(f"\n== {t}")


def short(text):
    return str(text).replace(DATA, "<casefiles>").replace(str(TMP), "<tmp>")


def last_error(e):
    import re
    m = re.search(r"\[DATA_SOURCE_PUSHDOWN_DISABLED\][^\n]*", str(e))
    if m:
        return m.group(0)[:150]
    lines = [l.strip() for l in str(e).splitlines() if l.strip()]
    keys = ("ValueError", "PySparkAssertionError", "AnalysisException", "DATA_SOURCE")
    return short(next((l for l in reversed(lines) if l.startswith(keys)), lines[0]))[:150]


head("1. What counts as a case")
print("on disk:     ", sorted(os.listdir(DATA)))
print("case folders:", cf.list_case_folders(DATA))

head("2. Two index versions, one shape")
for folder in ("CASE-0007", "case-0009", "CASE-0008", "CASE-0010"):
    with open(os.path.join(DATA, folder, "case.json"), "rb") as f:
        i = cf.parse_index(f.read(), folder)
    print({k: i[k] for k in ("case_id", "schema_version", "opened", "status", "priority", "tags", "problems")})

head("3. Partitions planned by bytes (first fit decreasing)")
print(cf.plan_by_bytes([("a", 50), ("b", 30), ("c", 30), ("d", 20), ("big", 500)], 60))
reader = cf.CaseFileReader({"path": DATA, "table": "attachments"}, None)
for p in reader.partitions():
    print(len(p.items), "files:", p.items[:3])

spark = session("ch73-demo", spark_sql_session_timeZone="UTC", spark_sql_shuffle_partitions="4")
spark.dataSource.register(cf.CaseFileSource)


def table(name, path=DATA, **opts):
    r = spark.read.format("casefile").option("table", name)
    for k, v in opts.items():
        r = r.option(k, v)
    return r.load(path)


head("4. Three tables")
table("cases").select("case_id", "folder", "schema_version", "opened", "status", "priority", "tags",
                      "problems").orderBy("case_id").show(truncate=False)
table("attachments").filter("status <> 'ok'").select("case_id", "name", "kind", "size", "status") \
    .orderBy("case_id").show(truncate=False)
table("changes").filter("case_id = 'CASE-0010'").select("line", "ts", "field", "old", "new", "actor", "raw",
                                                        "problem").orderBy("line").show(truncate=False)

head("5. Loose metadata as VARIANT")
table("cases").select("case_id",
                      F.expr("variant_get(meta, '$.customer.tier', 'string')").alias("tier"),
                      F.expr("try_variant_get(meta, '$.sla_hours', 'int')").alias("sla_hours"),
                      F.expr("schema_of_variant(meta)").alias("meta_schema")
                      ).orderBy("case_id").show(truncate=False)

head("6. Pushdown: opt-in, and what the plan shows")
flag = "spark.sql.python.filterPushdown.enabled"
print(flag, "=", spark.conf.get(flag))
print("default reader, flag off:", table("cases").filter("status = 'open'").count(), "open cases")
try:
    table("cases", pushdown="true").filter("status = 'open'").count()
except Exception as e:
    print("pushdown reader, flag off ->", last_error(e))
spark.conf.set(flag, "true")
audit = tempfile.mkdtemp(prefix="audit_")
q = (table("cases", pushdown="true", audit=audit)
     .filter("status = 'open' AND opened >= timestamp'2026-03-10 00:00:00'"))
rows = q.collect()
txt = q._jdf.queryExecution().explainString(
    spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted"))
print(next(l for l in txt.splitlines() if l.startswith("PushedFilters")))
print("rows:", sorted(r.case_id for r in rows), "| folders opened by tasks:",
      sorted({n.split("-", 2)[2].rsplit("-", 1)[0] for n in os.listdir(audit)}))
print("JVM default time zone:", spark._jvm.java.util.TimeZone.getDefault().getID(),
      "| session time zone:", spark.conf.get("spark.sql.session.timeZone"))

head("7. One DataFrame used twice, 4.3.0")
for opts in ({}, {"pushdown": "true"}):
    df = table("cases", **opts)
    seq = [df.count(), df.filter("status = 'open'").count(), df.count()]
    print(f"reader {'pushdown' if opts else 'default '}: count, filtered count, count again = {seq}")
c = cf.CaseFiles(spark, DATA, pushdown="true")
print("CaseFiles (a fresh load per table):",
      [c.table("cases").count(), c.table("cases").filter("status = 'open'").count(), c.table("cases").count()])

head("8. Writes publish by replacing one manifest")
out = TMP / "casefile-export"
shutil.rmtree(out, ignore_errors=True)
src = table("cases").select("case_id", "status", "priority", "opened", "tags", "meta")
src.filter("case_id <= 'CASE-0003'").write.format("casefile").mode("append").save(str(out))
print("after append:", sorted(os.listdir(out)))
print("_manifest.json:", json.dumps(cf.read_manifest(str(out))))
before = sorted(os.listdir(out))
broken = (src.filter("case_id > 'CASE-0005'")
          .withColumn("case_id", F.when(F.col("case_id") == "CASE-0010", F.lit("not a case"))
                      .otherwise(F.col("case_id"))).orderBy("case_id").coalesce(1))
for label, df, mode in (("a task that fails after four folders", broken, "append"),
                        ("a duplicate case_id", src.filter("case_id = 'CASE-0006'").union(
                            src.filter("case_id = 'CASE-0006'")).repartition(2), "append"),
                        ("an existing case_id", src.filter("case_id = 'CASE-0001'"), "append")):
    try:
        df.write.format("casefile").mode(mode).save(str(out))
    except Exception as e:
        print(f"{label}: {last_error(e)}")
    print("   folder unchanged:", sorted(os.listdir(out)) == before)
src.filter("case_id > 'CASE-0008'").write.format("casefile").mode("overwrite").save(str(out))
print("after overwrite:", sorted(os.listdir(out)))
print("_manifest.json:", json.dumps(cf.read_manifest(str(out))))

head("9. The changes table as a stream")
base = TMP / "casefile-stream"
shutil.rmtree(base, ignore_errors=True)
live, sink, ckpt = (str(base / d) for d in ("live", "sink", "ckpt"))
shutil.copytree(DATA, live)
query = (spark.readStream.format("casefile").option("table", "changes").option("maxBytesPerTrigger", 400)
         .load(live).writeStream.format("parquet").option("path", sink).option("checkpointLocation", ckpt)
         .trigger(availableNow=True).start())
query.awaitTermination()
print("batches (id, rows):", [(p.batchId, p.numInputRows) for p in query.recentProgress if p.numInputRows])
with open(os.path.join(ckpt, "offsets", "0")) as f:
    print("offsets/0, last line:", f.read().splitlines()[-1])
print("rows streamed:", spark.read.parquet(sink).count(), "| rows in a batch read:", table("changes", live).count())

head("10. Problems as warnings from the executors")
spark.conf.set("spark.sql.pyspark.worker.logging.enabled", "true")
table("cases").collect()
table("attachments").collect()
seen = set()
for _ in range(30):
    seen = {(r["level"], r["msg"]) for r in spark.tvf.python_worker_logs().filter("logger = 'casefile'").collect()}
    if len(seen) >= 8:
        break
    time.sleep(0.5)
for level, msg in sorted(seen, key=lambda x: x[1]):
    print(level, msg)
spark.stop()
