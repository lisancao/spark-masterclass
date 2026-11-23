"""Chapter 17: Spark against a standalone Hive Metastore 4.0.1 (start it with
ch17/standalone_metastore.sh up). The builtin 2.3.10 client fails, the 4.0.1 client from Maven works,
and two Spark processes use the metastore at the same time.

Run from code/:  uv run python ch17/remote_metastore.py > ch17/output/remote_metastore.txt
"""
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from hive import runner, describe  # noqa: E402

ROOT = HERE.parents[1]
W = ROOT / "data" / "work" / "ch17-hms"
URI = f"thrift://localhost:{os.environ.get('HMS_PORT', '19113')}"
SESSION = f"""
import sys, os
sys.path.insert(0, {str(HERE)!r})
from pyspark.sql import SparkSession
from hive import runner, describe
def make(version=None, ui="false"):
    b = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", ui)
         .config("spark.sql.warehouse.dir", {str(W / 'warehouse')!r})
         .config("spark.hadoop.hive.metastore.uris", {URI!r}))
    if version:
        b = (b.config("spark.sql.hive.metastore.version", version)
              .config("spark.sql.hive.metastore.jars", "maven"))
    s = b.enableHiveSupport().getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s
"""


def child(body, timeout=900):
    t0 = time.time()
    r = subprocess.run([sys.executable, "-c", SESSION + body], capture_output=True, text=True, timeout=timeout,
                       cwd=str(W))
    print(r.stdout, end="")
    if r.returncode:
        print([l for l in r.stderr.splitlines() if l.strip()][-1][:300])
    return time.time() - t0


print(f"== 1. the builtin client (Hive 2.3.10) against the 4.0.1 metastore at {URI}")
child("""
s = make()
q = runner(s)
q("SHOW DATABASES")
q("CREATE TABLE default.probe (id INT) STORED AS PARQUET")
""")

print("\n== 2. spark.sql.hive.metastore.version=4.0.1, spark.sql.hive.metastore.jars=maven")
from pyspark.sql import SparkSession  # noqa: E402
os.chdir(W)
t0 = time.time()
spark = (SparkSession.builder.master("local[2]").config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040"))
         .config("spark.sql.warehouse.dir", str(W / "warehouse"))
         .config("spark.hadoop.hive.metastore.uris", URI)
         .config("spark.sql.hive.metastore.version", "4.0.1")
         .config("spark.sql.hive.metastore.jars", "maven")
         .enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
q = runner(spark)
q("CREATE DATABASE IF NOT EXISTS taxi")
print(f"  first statement done ({'cached' if time.time() - t0 < 60 else 'downloaded'} client jars)")
q("DROP TABLE IF EXISTS taxi.zones")
q("CREATE TABLE taxi.zones (id INT, zone STRING) PARTITIONED BY (borough STRING) STORED AS PARQUET")
q("INSERT INTO taxi.zones PARTITION (borough = 'EWR') VALUES (1, 'Newark Airport')")
q("INSERT INTO taxi.zones PARTITION (borough = 'Queens') VALUES (2, 'Jamaica Bay'), (7, 'Astoria')")
q("SHOW PARTITIONS taxi.zones")
d = describe(spark, "taxi.zones", ["Type", "Provider", "Table Properties", "Location"])
for k, v in d.items():
    print(f"  {k:<17} {str(v).replace(str(W), '<ch17-hms>')}")

print("\n== 3. a second Spark process, while this one still runs")
child("""
s = make("4.0.1")
q = runner(s)
q("SELECT borough, count(*) AS zones FROM taxi.zones GROUP BY borough ORDER BY borough")
q("INSERT INTO taxi.zones PARTITION (borough = 'Bronx') VALUES (3, 'Allerton/Pelham Gardens')")
""")
q("REFRESH TABLE taxi.zones")
q("SHOW PARTITIONS taxi.zones")

print("== 4. DROP TABLE: the table was translated to EXTERNAL, with external.table.purge=TRUE")
folder = W / "warehouse" / "taxi.db" / "zones"
print("  folder before:", "exists" if folder.exists() else "gone")
q("DROP TABLE taxi.zones")
print("  folder after: ", "exists" if folder.exists() else "gone")
