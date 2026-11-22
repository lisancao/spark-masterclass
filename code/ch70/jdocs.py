"""Chapter 70: jdocs, a DSv2 connector in Java, driven and checked from PySpark (classic, local).

Build the jar first:   ch70/build.sh
Run from code/:        uv run python ch70/jdocs.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import JAR, docs_root, short, py_rows, make_pydocs, SCHEMA  # noqa: E402
from common.book import session, plan, ui_api  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = session("ch70-jdocs", spark_jars=str(JAR), spark_sql_session_timeZone="UTC")
spark.dataSource.register(make_pydocs())
Audit = spark._jvm.jdocs.Audit
root = docs_root()


def jdocs(**opts):
    r = spark.read.format("jdocs").option("path", root)
    for k, v in opts.items():
        r = r.option(k, v)
    return r.load()


def physical(df):
    """The physical plan text from the JVM QueryExecution (classic sessions only)."""
    return short(df._jdf.queryExecution().executedPlan().toString())


def scan_line(df):
    return next(l.strip() for l in physical(df).splitlines() if "Scan" in l)


def head(title):
    print(f"\n== {title}")


head("1. Full read: jdocs against the same source in Python")
full = jdocs()
print("schema:", full.schema.simpleString())
py = spark.read.format("pydocs").option("path", root).load()
diff = full.exceptAll(py).union(py.exceptAll(full)).count()
print(f"jdocs rows: {full.count()}, Python rows: {py.count()}, rows that differ: {diff}")

head("2. Short name in SQL")
spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW files USING jdocs OPTIONS (path '{root}')")
spark.sql("SELECT kind, count(*) AS files FROM files GROUP BY kind ORDER BY kind").show()

head("3. When each method runs")
Audit.reset()
bad = spark.read.format("jdocs").option("path", "/nope").load()
print("load() returned", bad.columns, "| scan builders so far:", Audit.builders())
try:
    bad.explain()
except Exception as e:
    print("explain() ->", type(e).__name__ + ":", str(e).splitlines()[0])
Audit.reset()
df = jdocs().filter("kind = 'pdf'")
plan(df); df.collect(); df.count()
print("explain + collect + count on one DataFrame: scan builders", Audit.builders(),
      "| listings", Audit.listings())

head("4. Column pruning")
Audit.reset()
df = jdocs().select("name", "size")
df.collect()
print(scan_line(df))
print("files opened:", Audit.opened())

head("5. Predicates as Spark offers them")
conds = ["kind = 'image' AND width > 12", "kind IN ('pdf', 'email')", "name LIKE '%.png'",
         "name LIKE 'red%'", "name LIKE '%ra%'", "kind <> 'image'", "size BETWEEN 10 AND 100",
         "width IS NULL", "lower(kind) = 'image'", "kind = 'audio' OR size < 100",
         "size % 2 = 1", "width > height", "modified > TIMESTAMP '2020-01-01 00:00:00'"]
for c in conds:
    Audit.reset()
    jdocs().filter(c).collect()
    print(f"{c}")
    for line in Audit.offered().splitlines():
        print("   ", line)

head("6. kind = 'image', pushed: the plan and the files opened")
Audit.reset()
df = jdocs().filter("kind = 'image'").select("name", "width")
rows = df.collect()
print(physical(df))
print("rows:", len(rows), "| files opened:", Audit.opened())

head("7. What jdocs cannot decide stays in Spark")
for c in ["kind = 'image' AND width > 12", "kind = 'audio' OR size < 100"]:
    df = jdocs().filter(c).select("name")
    print(c, "->", df.count(), "rows")
    print(physical(df))

head("8. LIMIT")
df = jdocs().select("name").limit(3)
print(physical(df))
df = jdocs().filter("width > 12").select("name").limit(2)
print("with a filter left after the scan:")
print(physical(df))

head("9. COUNT(*) without reading a file")
Audit.reset()
jobs_before = len(ui_api("jobs"))
df = jdocs().filter("kind = 'image'").groupBy().count()
print("count of images:", df.collect()[0][0], "| files opened:", Audit.opened(),
      "| Spark jobs run:", len(ui_api("jobs")) - jobs_before)
print(physical(df))
df = jdocs().groupBy("kind").count()
print(physical(df))
print("sum(size) is refused:")
print(physical(jdocs().groupBy("kind").agg(F.sum("size"))))

head("10. Statistics after pushdown")
for cbo in ("false", "true"):
    spark.conf.set("spark.sql.cbo.enabled", cbo)
    Audit.reset()
    df = jdocs().filter("kind = 'pdf'")
    stats = df._jdf.queryExecution().optimizedPlan().stats().toString()
    print(f"spark.sql.cbo.enabled={cbo}: {stats} | estimateSizeInBytes calls {Audit.sizeOnly()},"
          f" estimateStatistics calls {Audit.fullStats()}")
spark.conf.unset("spark.sql.cbo.enabled")

head("11. Reporting partitioning removes a shuffle")
for by_kind in ("false", "true"):
    df = jdocs(byKind=by_kind).groupBy("kind").agg(F.sum("size").alias("bytes"))
    df.collect()
    print(f"byKind={by_kind}")
    print(physical(df))

head("12. Runtime filters from a join (dynamic partition pruning)")
groups = spark.createDataFrame([("image", "media"), ("audio", "media"), ("pdf", "paper"),
                                ("email", "paper")], "kind string, grp string")
gpath = str(Path(root).parent / "masterclass-jdocs-groups")
groups.write.mode("overwrite").parquet(gpath)
Audit.reset()
g = spark.read.parquet(gpath).filter("grp = 'media'")
df = jdocs(rowBytes=1_000_000).join(g, "kind").select("name", "kind", "grp")
rows = df.collect()
print(short("\n".join(l for l in physical(df).splitlines() if "BatchScan" in l or "Join" in l)))
print("filter() received:", Audit.runtime())
print("rows:", len(rows), "kinds:", sorted({r.kind for r in rows}))

head("13. A custom metric in the Spark UI")
spark.sparkContext.setJobDescription("jdocs metric")
jdocs().select("name", "width").collect()
q = ui_api("sql?details=true")[-1]          # the last SQL execution: the query above
for node in q["nodes"]:
    for m in node.get("metrics", []):
        if m["name"].startswith("files opened"):
            print(node["nodeName"], "|", m["name"], "=", m["value"])

head("14. A wrong internal type fails in the task, not in planning")
df = jdocs(brokenName="true").select("name")
print("planned:", scan_line(df)[:60])
try:
    df.collect()
except Exception as e:
    import re
    print(re.search(r"java\.lang\.ClassCastException: [^\n(]*", str(e)).group(0))
spark.stop()
