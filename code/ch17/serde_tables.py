"""Chapter 17: Hive SerDe tables (STORED AS, ROW FORMAT) against data source tables (USING), what the
metastore keeps for each, what is on disk, and which reader and writer Spark uses.

Run from code/:  uv run python ch17/serde_tables.py > ch17/output/serde_tables.txt
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from hive import hive_session, runner, work, files, describe  # noqa: E402

W = work("ch17-serde")
spark = hive_session("ch17-serde", W)
q = runner(spark)
q("CREATE DATABASE taxi")
q("USE taxi")

print("== 1. five ways to make a table")
q("""CREATE TABLE z_text (id INT, borough STRING, zone STRING)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
     STORED AS TEXTFILE""")
q("CREATE TABLE z_hive_pq (id INT, borough STRING, zone STRING) STORED AS PARQUET")
q("CREATE TABLE z_hive_orc (id INT, borough STRING, zone STRING) STORED AS ORC")
q("""CREATE TABLE z_csv_serde (id STRING, borough STRING, zone STRING)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
     WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
     STORED AS TEXTFILE""")
q("CREATE TABLE z_ds_pq (id INT, borough STRING, zone STRING) USING parquet")
q("CREATE TABLE z_default (id INT, borough STRING, zone STRING)")
ROWS = "(1, 'EWR', 'Newark Airport'), (2, 'Queens', 'Jamaica Bay'), (4, 'Manhattan', 'Alphabet City')"
for t in ["z_text", "z_hive_pq", "z_hive_orc", "z_ds_pq", "z_default"]:
    spark.sql(f"INSERT INTO {t} VALUES {ROWS}")
spark.sql("INSERT INTO z_csv_serde VALUES ('1', 'EWR', 'Newark Airport'), ('2', 'Queens', 'Jamaica Bay')")

print("\n== 2. what the metastore keeps for each table")
print(f"  {'table':<12} {'Provider':<9} {'Serde Library':<55}")
for t in ["z_text", "z_hive_pq", "z_hive_orc", "z_csv_serde", "z_ds_pq", "z_default"]:
    d = describe(spark, t, ["Provider", "Serde Library", "InputFormat"])
    print(f"  {t:<12} {d['Provider']:<9} {d['Serde Library']}")

print("\n== 3. on disk")
files(W / "warehouse" / "taxi.db", "warehouse/taxi.db")
txt = sorted((W / "warehouse" / "taxi.db" / "z_text").glob("part-*"))[0]
print("  one file of z_text:", repr(txt.read_text()))
csv = sorted(p for p in (W / "warehouse" / "taxi.db" / "z_csv_serde").glob("part-*") if p.stat().st_size)[0]
print("  one file of z_csv_serde:", repr(csv.read_text()))


def scan(t):
    plan = spark.sql(f"EXPLAIN SELECT * FROM {t}").first()[0]
    leaf = [l for l in plan.splitlines() if "Scan" in l][0]
    return re.sub(r"^[\s+\-*()0-9]*", "", leaf).split(" spark_catalog")[0].split("[")[0].strip()


def write_cmd(t):
    plan = spark.sql(f"EXPLAIN INSERT INTO {t} SELECT * FROM z_ds_pq").first()[0]
    line = [l for l in plan.splitlines() if "Command" in l or "InsertInto" in l][0]
    return re.sub(r"^[\s+\-*()0-9]*", "", line).replace("Execute ", "").split(" ")[0]


print("\n== 4. the reader and the writer in the physical plan")
print(f"  {'table':<12} {'scan node':<16} write command")
for t in ["z_text", "z_hive_pq", "z_hive_orc", "z_ds_pq"]:
    print(f"  {t:<12} {scan(t):<16} {write_cmd(t)}")
spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
print(f"  with spark.sql.hive.convertMetastoreParquet=false:")
print(f"  {'z_hive_pq':<12} {scan('z_hive_pq'):<16} {write_cmd('z_hive_pq')}")
spark.conf.set("spark.sql.hive.convertMetastoreParquet", "true")

print("\n== 5. SHOW CREATE TABLE gives Spark DDL. AS SERDE gives Hive DDL")
for stmt in ["SHOW CREATE TABLE z_text", "SHOW CREATE TABLE z_text AS SERDE"]:
    print(f"> {stmt}")
    text = spark.sql(stmt).first()[0]
    print("  " + "\n  ".join(l for l in text.splitlines() if "transient_lastDdlTime" not in l
                             and l.strip() not in ("TBLPROPERTIES (", ")")))
ddl = spark.sql("SHOW CREATE TABLE z_text").first()[0].replace("taxi.z_text", "taxi.z_text_copy")
print("  run the Spark DDL under a new name, and it fails:")
q(ddl.split("TBLPROPERTIES")[0], show=False)
q("SHOW CREATE TABLE z_ds_pq AS SERDE")

print("\n== 6. USING hive, and the legacy default")
q("CREATE TABLE z_using_hive (id INT) USING hive OPTIONS (fileFormat 'orc')")
print(" ", describe(spark, "z_using_hive", ["Provider", "Serde Library"]))
spark.conf.set("spark.sql.legacy.createHiveTableByDefault", "true")
q("CREATE TABLE z_legacy (id INT)")
print(" ", describe(spark, "z_legacy", ["Provider", "Serde Library"]))
spark.conf.set("spark.sql.legacy.createHiveTableByDefault", "false")
q("CREATE TABLE z_char (code CHAR(3)) STORED AS PARQUET")
q("CREATE TABLE z_char_ds (code CHAR(3)) USING parquet")
