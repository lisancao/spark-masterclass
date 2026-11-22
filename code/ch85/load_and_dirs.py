"""Chapter 85: LOAD DATA (LOCAL copies, plain INPATH moves) and INSERT OVERWRITE DIRECTORY in its
Hive form (ROW FORMAT, STORED AS) and its data source form (USING).

Run from code/:  uv run python ch85/load_and_dirs.py > ch85/output/load_and_dirs.txt
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from hive import hive_session, runner, work, files  # noqa: E402

W = work("ch85-load")
spark = hive_session("ch85-load", W)
q = runner(spark)
T = W / "warehouse" / "taxi.db"
q("CREATE DATABASE taxi")
q("USE taxi")
inbox = W / "inbox"
inbox.mkdir()
(inbox / "zones_1.csv").write_text("1,EWR,Newark Airport\n2,Queens,Jamaica Bay\n")
(inbox / "zones_2.csv").write_text("3,Bronx,Allerton/Pelham Gardens\n4,Manhattan,Alphabet City\n")
(inbox / "zones_3.csv").write_text("5,Staten Island,Arden Heights\n")

print("== 1. LOAD DATA LOCAL INPATH copies a file into the table folder")
q("""CREATE TABLE zones (id INT, borough STRING, zone STRING)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE""")
q(f"LOAD DATA LOCAL INPATH '{inbox}/zones_1.csv' INTO TABLE zones")
files(inbox, "inbox")
files(T / "zones", "table folder")
q("SELECT * FROM zones ORDER BY id")

print("== 2. LOAD DATA INPATH (no LOCAL) moves the file")
q(f"LOAD DATA INPATH '{inbox}/zones_2.csv' INTO TABLE zones")
files(inbox, "inbox")
files(T / "zones", "table folder")

print("\n== 3. OVERWRITE replaces the files of the table")
q(f"LOAD DATA LOCAL INPATH '{inbox}/zones_3.csv' OVERWRITE INTO TABLE zones")
files(T / "zones", "table folder")
q("SELECT * FROM zones")

print("== 4. into one partition, and into a data source table")
q("""CREATE TABLE zones_p (id INT, borough STRING, zone STRING) PARTITIONED BY (load_day STRING)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE""")
q(f"LOAD DATA LOCAL INPATH '{inbox}/zones_1.csv' INTO TABLE zones_p PARTITION (load_day = '2026-06-01')")
files(T / "zones_p", "table folder")
q("CREATE TABLE zones_ds (id INT, borough STRING, zone STRING) USING csv")
q(f"LOAD DATA LOCAL INPATH '{inbox}/zones_1.csv' INTO TABLE zones_ds")
q("""CREATE TABLE zones_pq (id INT, borough STRING, zone STRING) STORED AS PARQUET""")
q(f"LOAD DATA LOCAL INPATH '{inbox}/zones_1.csv' INTO TABLE zones_pq")
q("SELECT * FROM zones_pq")

print("== 5. INSERT OVERWRITE DIRECTORY: the Hive form and the data source form")
out = W / "export"
stmts = {
    "hive_delimited": f"""INSERT OVERWRITE LOCAL DIRECTORY '{out}/hive_delimited'
     ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
     SELECT * FROM zones_p""",
    "hive_parquet": f"""INSERT OVERWRITE LOCAL DIRECTORY '{out}/hive_parquet'
     STORED AS PARQUET
     SELECT * FROM zones_p""",
    "ds_json": f"""INSERT OVERWRITE DIRECTORY '{out}/ds_json'
     USING json OPTIONS (compression 'none')
     SELECT * FROM zones_p""",
}
for name, s in stmts.items():
    plan = spark.sql("EXPLAIN " + s).first()[0]
    cmd = [re.sub(r"^[\s+\-*()0-9]*(Execute )?", "", l).split(" ")[0] for l in plan.splitlines()
           if "Command" in l][0]
    q(s, show=False)
    print(f"  command: {cmd}")
    files(out / name, name)
print("  one file of hive_delimited:",
      repr([p for p in sorted((out / "hive_delimited").glob("part-*")) if p.stat().st_size][0].read_text()))
spark.conf.set("spark.sql.hive.convertMetastoreInsertDir", "false")
plan = spark.sql("EXPLAIN " + stmts["hive_parquet"]).first()[0]
print("  with spark.sql.hive.convertMetastoreInsertDir=false, STORED AS PARQUET runs:",
      [re.sub(r"^[\s+\-*()0-9]*(Execute )?", "", l).split(" ")[0] for l in plan.splitlines() if "Command" in l][0])
