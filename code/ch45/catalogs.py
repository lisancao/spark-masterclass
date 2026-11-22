"""Chapter 45: four Iceberg catalogs in one session, and where each one keeps the table pointer.

Needs the two containers:  code/ch45/catalogs.sh up
Run from code/:  uv run python ch45/catalogs.py
"""
import json
import shutil
import sqlite3
import sys
import urllib.parse
import urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import ZONES
from ice import WORK, err, ice_session, tidy

CATALOGS = {"lake": "hadoop", "jdbc": "jdbc", "hive": "hive", "rest": "rest"}
shutil.rmtree(WORK / "hadoop", ignore_errors=True)
shutil.rmtree(WORK / "jdbc", ignore_errors=True)

# spark_catalog stays Spark's own session catalog, here backed by the same Hive Metastore.
spark = ice_session("ch45-catalogs", CATALOGS, **{
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.hive.metastore.uris": "thrift://localhost:19083",
    "spark.sql.warehouse.dir": f"file://{WORK}/hive",
})
for t in ("hive.nyc.zones", "hive.nyc.zones_tmp", "rest.nyc.zones", "rest.nyc.zones_tmp",
          "rest.nyc.zones_by_borough"):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")        # tables left by an earlier run
    except Exception:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
for old in [*(WORK / "hive" / "nyc.db").glob("zones*"), *(WORK / "polaris" / "nyc").glob("zones*")]:
    shutil.rmtree(old)                                      # folders of tables dropped without PURGE
zones = spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))
zones.createOrReplaceTempView("zones_csv")

print("== One table in each catalog")
for name in CATALOGS:
    spark.sql(f"DROP TABLE IF EXISTS {name}.nyc.zones")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}.nyc")
    spark.sql(f"CREATE TABLE {name}.nyc.zones USING iceberg AS SELECT * FROM zones_csv")
    n = spark.sql(f"SELECT count(*) FROM {name}.nyc.zones").first()[0]
    loc = spark.sql(f"DESCRIBE TABLE EXTENDED {name}.nyc.zones").where("col_name = 'Location'").first()[1]
    print(f"{name:<5} ({CATALOGS[name]:<6}) {n} rows  {tidy(loc)}")

print("\nSHOW CATALOGS:", ", ".join(r[0] for r in spark.sql("SHOW CATALOGS").collect()))

print("\n== Where each catalog keeps the pointer to the current metadata file")
hint = WORK / "hadoop" / "nyc" / "zones" / "metadata" / "version-hint.text"
print(f"lake: the file {tidy(hint)} contains {hint.read_text().strip()!r}")
print("      so the current metadata file is",
      [p.name for p in hint.parent.glob("v*.metadata.json")])

db = sqlite3.connect(WORK / "jdbc" / "catalog.db")
row = db.execute("SELECT catalog_name, table_namespace, table_name, metadata_location "
                 "FROM iceberg_tables WHERE table_name = 'zones'").fetchone()
print(f"jdbc: a row in the SQLite table iceberg_tables:\n      {tidy(row)}")

props = dict(spark.sql("SHOW TBLPROPERTIES spark_catalog.nyc.zones").collect())
print("hive: table parameters in the Hive Metastore:")
print(f"      metadata_location = {tidy(props['metadata_location'])}")
print(f"      other keys: {', '.join(k for k in sorted(props) if k != 'metadata_location')}")

base = "http://localhost:18181/api/catalog"
token = json.load(urllib.request.urlopen(f"{base}/v1/oauth/tokens", data=urllib.parse.urlencode(
    {"grant_type": "client_credentials", "client_id": "root", "client_secret": "s3cr3t",
     "scope": "PRINCIPAL_ROLE:ALL"}).encode()))["access_token"]
def rest(path):
    req = urllib.request.Request(f"{base}{path}", headers={"Authorization": f"Bearer {token}"})
    return json.load(urllib.request.urlopen(req))
cfg = rest("/v1/config?warehouse=taxi")
prefix = cfg["overrides"]["prefix"]
print(f"rest: GET /v1/config?warehouse=taxi -> prefix {prefix!r}, "
      f"{len(cfg.get('endpoints', []))} endpoints")
t = rest(f"/v1/{prefix}/namespaces/nyc/tables/zones")
print(f"      GET /v1/{prefix}/namespaces/nyc/tables/zones -> metadata-location:\n"
      f"      {tidy(t['metadata-location'])}")

print("\n== spark_catalog (Spark's Hive session catalog) reads the Hive table as a plain table")
try:
    spark.sql("SELECT * FROM spark_catalog.nyc.zones LIMIT 1").collect()
    print("spark_catalog.nyc.zones: read worked")
except Exception as e:
    print("spark_catalog.nyc.zones:", err(e, 150))

print("\n== Namespaces")
for name in CATALOGS:
    try:
        spark.sql(f"ALTER NAMESPACE {name}.nyc SET PROPERTIES ('team' = 'data-eng')")
        print(f"{name:<5} ALTER NAMESPACE ... SET PROPERTIES: works")
    except Exception as e:
        print(f"{name:<5} ALTER NAMESPACE ... SET PROPERTIES: {err(e, 70)}")
print("DESCRIBE NAMESPACE EXTENDED rest.nyc:")
for r in spark.sql("DESCRIBE NAMESPACE EXTENDED rest.nyc").collect():
    if r[0] != "Owner":
        print(f"  {r[0]:<20} {tidy(r[1])}")
for name in CATALOGS:
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}.nyc.raw")
        print(f"{name:<5} nested namespace nyc.raw: created,",
              [r[0] for r in spark.sql(f"SHOW NAMESPACES IN {name}.nyc").collect()])
    except Exception as e:
        print(f"{name:<5} nested namespace nyc.raw: {err(e, 90)}")

print("\n== One query over three catalogs")
spark.sql("CREATE OR REPLACE TABLE rest.nyc.zones_by_borough USING iceberg AS "
          "SELECT h.Borough, count(*) AS zones FROM hive.nyc.zones h "
          "JOIN jdbc.nyc.zones j ON h.LocationID = j.LocationID GROUP BY h.Borough")
spark.sql("SELECT * FROM rest.nyc.zones_by_borough ORDER BY zones DESC").show()

print("== DROP TABLE and DROP TABLE ... PURGE, and what stays on disk")
def drop(name, table, purge):
    folder = Path(spark.sql(f"DESCRIBE TABLE EXTENDED {name}.nyc.{table}")
                  .where("col_name = 'Location'").first()[1].removeprefix("file:").removeprefix("//"))
    files = sum(1 for p in folder.rglob("*") if p.is_file() and p.suffix != ".crc")
    try:
        spark.sql(f"DROP TABLE {name}.nyc.{table}" + (" PURGE" if purge else ""))
    except Exception as e:
        return f"{err(e, 80)}"
    left = sum(1 for p in folder.rglob("*") if p.is_file() and p.suffix != ".crc") if folder.exists() else 0
    return f"{files} files before, {left} after"
for name in CATALOGS:
    spark.sql(f"CREATE TABLE {name}.nyc.zones_tmp USING iceberg AS SELECT * FROM zones_csv")
    print(f"{name:<5} DROP TABLE:         {drop(name, 'zones', False)}")
    print(f"{name:<5} DROP TABLE ... PURGE: {drop(name, 'zones_tmp', True)}")
spark.sql("DROP TABLE rest.nyc.zones_by_borough PURGE")
