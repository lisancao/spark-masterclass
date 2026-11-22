"""Chapter 29: a Delta table, its transaction log on disk, MERGE, time travel and the guarantees.

Delta Lake has no release for Spark 4.3 yet (delta-spark 4.4.1 requires pyspark<=4.2.0, and its jar
fails on 4.3.0, see delta_on_43.txt). This script runs on the closest supported pairing,
Spark 4.2.0 with Delta 4.4.1, in its own environment:

  uv run --no-project --with pyspark==4.2.0 --with delta-spark==4.4.1 python ch29/delta_log.py
"""
import json
import os
import re
import shutil
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

ROOT = Path(os.environ.get("BOOK_DATA", Path(__file__).resolve().parents[2] / "data")) / "work" / "part06" / "ch29-delta"
shutil.rmtree(ROOT, ignore_errors=True)
builder = (SparkSession.builder.master("local[4]").appName("ch29-delta")
           .config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040"))
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("spark.sql.session.timeZone", "UTC")
           .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")   # Delta reads TIMESTAMP AS OF in the JVM zone
           .config("spark.sql.shuffle.partitions", "2"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("OFF")
print("Spark", spark.version, "with", [j.split("/")[-1] for j in spark.conf.get("spark.jars.packages").split(",")])

path = str(ROOT / "employees")
T = f"delta.`{path}`"


def section(title):
    print(f"\n== {title}")


def condition(exc):
    """The error condition in a Delta or Spark exception, from its message."""
    m = re.search(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)?)\]", str(exc))
    return m.group(1) if m else type(exc).__name__


def log_files():
    return sorted(p.name for p in (Path(path) / "_delta_log").iterdir()
                  if p.is_file() and not p.name.startswith("."))


def show_commit(version):
    """Print each action of one commit file, shortened to the fields that matter."""
    for line in open(Path(path) / "_delta_log" / f"{version:020d}.json"):
        action, body = next(iter(json.loads(line).items()))
        if action == "commitInfo":
            keep = {k: body[k] for k in ("operation", "operationParameters", "operationMetrics") if k in body}
        elif action == "metaData":
            keep = {"schema": [f["name"] + ":" + f["type"] for f in json.loads(body["schemaString"])["fields"]],
                    "configuration": body["configuration"]}
        elif action in ("add", "remove"):
            keep = {"path": body["path"][:18] + "...", "dataChange": body.get("dataChange")}
            if "stats" in body and body["stats"]:
                s = json.loads(body["stats"])
                keep["stats"] = {"numRecords": s.get("numRecords"), "minValues": s.get("minValues"),
                                 "maxValues": s.get("maxValues")}
        else:
            keep = body
        print(f"  {action:<10} {json.dumps(keep)}")


SCHEMA = "id INT, name STRING, department STRING, salary DOUBLE"

section("version 0: write three rows, with the change data feed on")
(spark.createDataFrame([(1, "Alice", "Engineering", 85000.0), (2, "Bob", "Marketing", 65000.0),
                        (3, "Charlie", "Sales", 55000.0)], SCHEMA)
 .coalesce(1).write.format("delta").option("delta.enableChangeDataFeed", "true").save(path))
print("data files:", sorted(p.name[:18] + "..." for p in Path(path).glob("*.parquet")))
print("_delta_log:", log_files())
show_commit(0)

section("version 1: MERGE a raise for Bob and two new hires")
spark.createDataFrame([(2, "Bob", "Marketing", 72000.0), (4, "Diana", "Engineering", 92000.0),
                       (5, "Eve", "Marketing", 70000.0)], SCHEMA).createOrReplaceTempView("updates")
spark.sql(f"""MERGE INTO {T} t USING updates s ON t.id = s.id
              WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
show_commit(1)
after_v1 = spark.sql("SELECT CAST(current_timestamp() AS STRING)").first()[0]   # session time zone: UTC

section("version 2: DELETE Charlie")
spark.sql(f"DELETE FROM {T} WHERE id = 3")
show_commit(2)

section("the table now, and the trap of reading the folder as Parquet")
spark.read.format("delta").load(path).orderBy("id").show()
print("rows read as delta:  ", spark.read.format("delta").load(path).count())
print("rows read as parquet:", spark.read.parquet(path).count(),
      f"(every file still on disk: {len(list(Path(path).glob('*.parquet')))})")

section("time travel")
spark.read.format("delta").option("versionAsOf", 0).load(path).orderBy("id").show()
ts = after_v1
print(f"TIMESTAMP AS OF '{ts}' ->",
      sorted((r.id, r.salary) for r in spark.sql(f"SELECT * FROM {T} TIMESTAMP AS OF '{ts}'").collect()))
try:
    spark.sql(f"SELECT * FROM {T} VERSION AS OF 99").collect()
except Exception as e:
    print("VERSION AS OF 99 ->", condition(e), str(e).split("] ", 1)[1].split("\n")[0][:80])

section("DESCRIBE HISTORY")
(spark.sql(f"DESCRIBE HISTORY {T}").orderBy("version")
 .select("version", "operation", F.to_json("operationMetrics").alias("operationMetrics"))
 .show(truncate=88))

section("the change data feed from version 1")
(spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 1).load(path)
 .select("id", "name", "salary", "_change_type", "_commit_version")
 .orderBy("_commit_version", "id", "_change_type").show())

section("schema enforcement on appends")
def version():
    return spark.sql(f"DESCRIBE HISTORY {T}").agg(F.max("version")).first()[0]


cases = [
    ("an extra column", "id INT, name STRING, department STRING, salary DOUBLE, country STRING", {}),
    ("an extra column, mergeSchema", "id INT, name STRING, department STRING, salary DOUBLE, country STRING",
     {"mergeSchema": "true"}),
    ("salary as a string, mergeSchema", "id INT, name STRING, department STRING, salary STRING",
     {"mergeSchema": "true"}),
    ("id as BIGINT into the INT column", "id BIGINT, name STRING, department STRING, salary DOUBLE", {}),
    ("only id and name", "id INT, name STRING", {}),
]
for label, schema, opts in cases:
    row = [tuple([6, "Frank", "Sales", 50000.0, "CA"][:len(schema.split(","))])]
    if "salary STRING" in schema:
        row = [(6, "Frank", "Sales", "50k")]
    v = version()
    try:
        w = spark.createDataFrame(row, schema).write.format("delta").mode("append")
        for k, val in opts.items():
            w = w.option(k, val)
        w.save(path)
        result = f"OK, version {version()}"
    except Exception as e:
        result = f"{condition(e)}, still version {version()}"
    print(f"  {label:<34} {result}")
print("  columns now:", spark.read.format("delta").load(path).columns)

section("constraints")
cpath = str(ROOT / "orders")
C = f"delta.`{cpath}`"
spark.sql(f"CREATE TABLE {C} (order_id STRING NOT NULL, quantity INT) USING delta")
spark.sql(f"ALTER TABLE {C} ADD CONSTRAINT quantity_positive CHECK (quantity > 0)")
for values in ["('o-1', 2)", "('o-2', -2)", "('o-3', NULL)", "(NULL, 1)"]:
    try:
        spark.sql(f"INSERT INTO {C} VALUES {values}")
        print(f"  insert {values:<12} OK")
    except Exception as e:
        print(f"  insert {values:<12} {condition(e)}")
try:
    spark.sql(f"ALTER TABLE {C} ALTER COLUMN quantity SET NOT NULL")
except Exception as e:
    print("  SET NOT NULL later ->", condition(e), str(e).split("] ", 1)[-1].split("\n")[0][:60])
props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {C}").collect()}
print("  stored as a table property:", {k: v for k, v in props.items() if "constraint" in k})

section("MERGE: two source rows for one target row, and new source columns")
mpath = str(ROOT / "events")
M = f"delta.`{mpath}`"
spark.createDataFrame([("a", 1.0), ("b", 2.0)], "event_id STRING, amount DOUBLE").write.format("delta").save(mpath)
spark.createDataFrame([("a", 10.0), ("a", 11.0)], "event_id STRING, amount DOUBLE").createOrReplaceTempView("twice")
try:
    spark.sql(f"MERGE INTO {M} t USING twice s ON t.event_id = s.event_id WHEN MATCHED THEN UPDATE SET *")
except Exception as e:
    print("  two source rows match one target row ->", condition(e))
    print("   ", [l.strip() for l in str(e).splitlines() if "DELTA_" in l][0][:120])
spark.createDataFrame([("c", 3.0, "CA")], "event_id STRING, amount DOUBLE, country STRING"
                      ).createOrReplaceTempView("wider")
spark.sql(f"MERGE INTO {M} t USING wider s ON t.event_id = s.event_id WHEN NOT MATCHED THEN INSERT *")
print("  INSERT * without evolution ->", spark.read.format("delta").load(mpath).columns,
      spark.read.format("delta").load(mpath).count(), "rows")
spark.createDataFrame([("d", 4.0, "VN")], "event_id STRING, amount DOUBLE, country STRING"
                      ).createOrReplaceTempView("wider2")
spark.sql(f"MERGE WITH SCHEMA EVOLUTION INTO {M} t USING wider2 s ON t.event_id = s.event_id "
          "WHEN NOT MATCHED THEN INSERT *")
spark.read.format("delta").load(mpath).orderBy("event_id").show()

section("idempotent writes")
n = len([f for f in (Path(mpath) / "_delta_log").glob("*.json")])
spark.sql(f"MERGE INTO {M} t USING wider2 s ON t.event_id = s.event_id WHEN NOT MATCHED THEN INSERT *")
print("  the same MERGE again (nothing new to insert) adds commits:",
      len([f for f in (Path(mpath) / "_delta_log").glob("*.json")]) - n)
bpath = str(ROOT / "bronze")
day1 = spark.createDataFrame([("orders_2026-10-01.json", i) for i in range(6)], "file STRING, line INT")
for attempt in (1, 2):
    (day1.write.format("delta").mode("overwrite")
     .option("replaceWhere", "file = 'orders_2026-10-01.json'").save(bpath))
    print(f"  replaceWhere, attempt {attempt}: {spark.read.format('delta').load(bpath).count()} rows")
for attempt in (1, 2):
    (day1.write.format("delta").mode("append")
     .option("txnAppId", "ingest").option("txnVersion", 7).save(bpath))
    print(f"  append with txnVersion 7, attempt {attempt}: {spark.read.format('delta').load(bpath).count()} rows")
spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", json.dumps({"file": "day2", "quarantined": 0}))
try:
    spark.sql(f"DELETE FROM delta.`{bpath}` WHERE line = 5")
finally:
    spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
(spark.sql(f"DESCRIBE HISTORY delta.`{bpath}`").orderBy("version")
 .select("version", "operation", "userMetadata").show(truncate=False))

section("RESTORE and VACUUM")
spark.sql(f"RESTORE TABLE {T} TO VERSION AS OF 1")
print("  after RESTORE to version 1:", sorted(r.id for r in spark.read.format("delta").load(path).collect()))
print("  files on disk before VACUUM:", len(list(Path(path).glob("*.parquet"))))
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM {T} RETAIN 0 HOURS")
print("  files on disk after VACUUM RETAIN 0 HOURS:", len(list(Path(path).glob("*.parquet"))))
try:
    spark.read.format("delta").option("versionAsOf", 0).load(path).collect()
    print("  version 0 still readable")
except Exception as e:
    print("  version 0 after VACUUM ->", condition(e))

section("checkpoints: commit until the log writes one")
for i in range(12):
    spark.sql(f"INSERT INTO delta.`{bpath}` VALUES ('extra.json', {i})")
names = sorted(p.name for p in (Path(bpath) / "_delta_log").iterdir() if p.is_file() and not p.name.startswith("."))
print("  commits:", len([n for n in names if n.endswith(".json")]),
      " checkpoints:", [n for n in names if "checkpoint" in n])
print("  _last_checkpoint:", (Path(bpath) / "_delta_log" / "_last_checkpoint").read_text().strip()[:80])
