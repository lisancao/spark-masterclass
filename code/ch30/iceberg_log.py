"""Chapter 30: an Iceberg table on Spark 4.3.0, its metadata tree on disk, MERGE and time travel.

No released Iceberg version supports Spark 4.3 yet. This uses an unreleased runtime built from
apache/iceberg PR #18749 (commit b1eab64, "Spark: Initial support for 4.3.0"); notes/part06.md
has the build and its sha256. Run from code/:  uv run python ch30/iceberg_log.py
"""
import json
import re
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.lake import lake_session, WORK, tidy

warehouse = WORK / "ch30-iceberg"
shutil.rmtree(warehouse, ignore_errors=True)
spark = lake_session("ch30-iceberg", warehouse)
spark.sparkContext.setLogLevel("ERROR")
table_dir = warehouse / "hr" / "employees"
print("Spark", spark.version, "with", Path(str(spark.conf.get("spark.jars"))).name)


def section(title):
    print(f"\n== {title}")


def listing():
    for sub in ("metadata", "data"):
        names = sorted(p.name for p in (table_dir / sub).iterdir() if not p.name.startswith("."))
        print(f"  {sub}/")
        for n in names:
            print(f"    {n if len(n) < 60 else n[:28] + '...' + n[-24:]}")


def metadata_json():
    v = (table_dir / "metadata" / "version-hint.text").read_text().strip()
    return v, json.loads((table_dir / "metadata" / f"v{v}.metadata.json").read_text())


SCHEMA = "id INT, name STRING, department STRING, salary DOUBLE"
section("create, then insert three rows")
spark.sql(f"CREATE TABLE lake.hr.employees ({SCHEMA}) USING iceberg")
(spark.createDataFrame([(1, "Alice", "Engineering", 85000.0), (2, "Bob", "Marketing", 65000.0),
                        (3, "Charlie", "Sales", 55000.0)], SCHEMA).coalesce(1)
 .writeTo("lake.hr.employees").append())
listing()

section("the current metadata file (the root of the tree)")
v, md = metadata_json()
print(f"  version-hint.text -> {v}, so the table is v{v}.metadata.json")
print("  format-version:", md["format-version"], " current-snapshot-id:", md["current-snapshot-id"])
for s in md["snapshots"]:
    print("  snapshot", s["snapshot-id"], s["summary"]["operation"],
          "manifest-list:", Path(s["manifest-list"]).name[:30] + "...")
print("  schema fields:", [f"{f['id']}:{f['name']}:{f['type']}" for f in md["schemas"][-1]["fields"]])

section("MERGE a raise for Bob and two new hires, then DELETE Charlie")
spark.createDataFrame([(2, "Bob", "Marketing", 72000.0), (4, "Diana", "Engineering", 92000.0),
                       (5, "Eve", "Marketing", 70000.0)], SCHEMA).createOrReplaceTempView("updates")
spark.sql("""MERGE INTO lake.hr.employees t USING updates s ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
spark.sql("DELETE FROM lake.hr.employees WHERE id = 3")
spark.table("lake.hr.employees").orderBy("id").show()
listing()

section("the tree, read through Iceberg's metadata tables")
spark.sql("""SELECT snapshot_id, parent_id, operation,
                    summary['added-data-files'] AS added_files, summary['deleted-data-files'] AS deleted_files,
                    summary['total-records'] AS records
             FROM lake.hr.employees.snapshots ORDER BY committed_at""").show(truncate=False)
spark.sql("""SELECT content, regexp_extract(file_path, '[^/]+$', 0) AS file, record_count
             FROM lake.hr.employees.files""").show(truncate=False)
spark.sql("""SELECT regexp_extract(path, '[^/]+$', 0) AS manifest, added_data_files_count AS added,
                    existing_data_files_count AS existing, deleted_data_files_count AS deleted
             FROM lake.hr.employees.manifests""").show(truncate=False)

section("time travel by snapshot id and by timestamp")
snaps = spark.sql("SELECT snapshot_id, CAST(committed_at AS STRING) AS committed_at FROM lake.hr.employees.snapshots ORDER BY committed_at").collect()
first = snaps[0].snapshot_id
print(f"VERSION AS OF {first}:",
      sorted((r.id, r.salary) for r in spark.sql(f"SELECT * FROM lake.hr.employees VERSION AS OF {first}").collect()))
ts = snaps[1].committed_at   # a string from Spark, in the session time zone (UTC)
print(f"TIMESTAMP AS OF '{ts}':",
      sorted((r.id, r.salary) for r in spark.sql(f"SELECT * FROM lake.hr.employees TIMESTAMP AS OF '{ts}'").collect()))
print(f"new in 4.3, employees@v{first}:",
      sorted(r.id for r in spark.sql(f"SELECT * FROM lake.hr.employees@v{first}").collect()))
try:
    spark.read.option("snapshot-id", first).table("lake.hr.employees").count()
except Exception as e:
    print("option snapshot-id ->", str(e).split("\n")[0])
print("option versionAsOf:", spark.read.option("versionAsOf", first).table("lake.hr.employees").count(), "rows")
try:
    spark.sql("SELECT * FROM lake.hr.employees VERSION AS OF 99").collect()
except Exception as e:
    print("VERSION AS OF 99 ->", type(e).__name__, str(e).split("\n")[0][:90])

section("copy-on-write versus merge-on-read")
spark.sql("ALTER TABLE lake.hr.employees SET TBLPROPERTIES ('write.delete.mode'='merge-on-read', "
          "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')")
spark.sql("DELETE FROM lake.hr.employees WHERE id = 5")
spark.sql("""SELECT content, regexp_extract(file_path, '[^/]+$', 0) AS file, record_count
             FROM lake.hr.employees.files""").show(truncate=False)
print("content 0 is a data file, content 1 a position delete file: the DELETE wrote a small file")
print("that lists the deleted row, and left the data file in place.")

section("schema evolution, and the 4.x SQL that asks for it")
spark.createDataFrame([(6, "Frank", "Sales", 50000.0, "CA")], SCHEMA + ", country STRING"
                      ).createOrReplaceTempView("wider")
try:
    spark.sql("INSERT INTO lake.hr.employees SELECT * FROM wider")
except Exception as e:
    print("INSERT INTO with an extra column ->", e.getCondition())
spark.sql("INSERT WITH SCHEMA EVOLUTION INTO lake.hr.employees SELECT * FROM wider")
print("INSERT WITH SCHEMA EVOLUTION -> columns", spark.table("lake.hr.employees").columns)
_, md = metadata_json()
print("  field ids:", [f"{f['id']}:{f['name']}" for f in md["schemas"][-1]["fields"]])

section("DataFrame.mergeInto")
(spark.createDataFrame([(1, "Alice", "Engineering", 90000.0, "CA")], SCHEMA + ", country STRING").alias("s")
 .mergeInto("lake.hr.employees", F.expr("lake.hr.employees.id = s.id"))
 .whenMatched().updateAll().whenNotMatched().insertAll().merge())
spark.table("lake.hr.employees").orderBy("id").show()

section("history (the snapshot log)")
spark.sql("SELECT made_current_at, snapshot_id, parent_id FROM lake.hr.employees.history").show(truncate=False)

section("the plan of a MERGE: copy-on-write against merge-on-read")
spark.createDataFrame([(2, "Bob", "Marketing", 75000.0, None), (7, "Gia", "Sales", 61000.0, "VN")],
                      SCHEMA + ", country STRING").createOrReplaceTempView("upd")
merge_sql = """MERGE INTO lake.hr.employees t USING upd s ON t.id = s.id
               WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"""
for mode in ("copy-on-write", "merge-on-read"):
    spark.sql(f"ALTER TABLE lake.hr.employees SET TBLPROPERTIES ('write.merge.mode'='{mode}')")
    p = spark.sql("EXPLAIN " + merge_sql).first()[0]
    ops = [l.strip()[:96] for l in p.split("== Physical Plan ==")[1].splitlines()
           if any(k in l for k in ("ReplaceData", "WriteDelta", "MergeRows", "Join", "BatchScan"))]
    print(f"  {mode}:")
    for o in ops:
        print("    " + tidy(o))

section("WHEN NOT MATCHED BY SOURCE: a full sync in one statement")
spark.sql("CREATE TABLE lake.hr.roster (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO lake.hr.roster VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
spark.createDataFrame([(2, "Bob"), (3, "Charles"), (4, "Diana")], "id INT, name STRING") \
    .createOrReplaceTempView("hr_feed")
spark.sql("""MERGE INTO lake.hr.roster t USING hr_feed s ON t.id = s.id
                 WHEN MATCHED AND t.name <> s.name THEN UPDATE SET name = s.name
                 WHEN NOT MATCHED THEN INSERT *
                 WHEN NOT MATCHED BY SOURCE THEN DELETE""")
print("  roster now:", [(x.id, x.name) for x in spark.table("lake.hr.roster").orderBy("id").collect()])

section("replace a slice: INSERT ... REPLACE WHERE, and the 4.2 REPLACE USING grammar")
spark.sql("CREATE TABLE lake.hr.daily (day DATE, n INT) USING iceberg PARTITIONED BY (day)")
spark.sql("INSERT INTO lake.hr.daily VALUES (DATE'2026-10-01', 1), (DATE'2026-10-02', 2)")
for attempt in (1, 2):
    spark.sql("""INSERT INTO lake.hr.daily REPLACE WHERE day = DATE'2026-10-02'
                 SELECT DATE'2026-10-02', 20 UNION ALL SELECT DATE'2026-10-02', 21""")
    print(f"  REPLACE WHERE, attempt {attempt}:",
          [(str(x.day), x.n) for x in spark.table("lake.hr.daily").orderBy("day", "n").collect()])
try:
    spark.sql("INSERT INTO lake.hr.daily REPLACE USING (day) SELECT DATE'2026-10-02', 22")
    print("  REPLACE USING: worked")
except Exception as e:
    print("  REPLACE USING ->", e.getCondition())

section("two writers at once: optimistic concurrency")
from concurrent.futures import ThreadPoolExecutor
spark.sql("CREATE TABLE lake.hr.accounts (id INT, balance DOUBLE) USING iceberg")
spark.createDataFrame([(i, 100.0) for i in range(1, 7)], "id INT, balance DOUBLE").coalesce(1) \
    .writeTo("lake.hr.accounts").append()


def attempt_sql(stmt):
    try:
        spark.sql(stmt)
        return "committed"
    except Exception as e:
        # Classic PySpark wraps the JVM error; the Iceberg exception is a line inside the message
        java = [l.strip(": ") for l in str(e).splitlines() if l.strip(": ").startswith("org.apache.iceberg")]
        return re.sub(r"\[file:[^\]]*/", "[.../", java[0] if java else type(e).__name__)


def n_snapshots():
    return spark.sql("SELECT count(*) FROM lake.hr.accounts.snapshots").first()[0]


for label, stmts in [
    ("two appends", ["INSERT INTO lake.hr.accounts VALUES (7, 1.0)",
                     "INSERT INTO lake.hr.accounts VALUES (8, 1.0)"]),
    ("two deletes on the same file", ["DELETE FROM lake.hr.accounts WHERE id = 1",
                                      "DELETE FROM lake.hr.accounts WHERE id = 2"]),
]:
    before = n_snapshots()
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(attempt_sql, stmts))
    print(f"  {label}: snapshots {before} -> {n_snapshots()}")
    for r in results:
        print("    " + r)
print("  rows now:", sorted(r.id for r in spark.table("lake.hr.accounts").collect()))
