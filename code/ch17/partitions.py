"""Chapter 17: partitions in the metastore. Static and dynamic inserts, folders that another job
writes, MSCK REPAIR TABLE and RECOVER PARTITIONS, partition DDL, and INSERT OVERWRITE on Hive tables.

Run from code/:  uv run python ch17/partitions.py > ch17/output/partitions.txt
Then ch17/derby_rows.sh reads the rows that this script left in the Derby metastore.
"""
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from hive import hive_session, runner, work, files, describe  # noqa: E402

W = work("ch17-partitions")
spark = hive_session("ch17-partitions", W)
q = runner(spark)
T = W / "warehouse" / "taxi.db"
q("CREATE DATABASE taxi")
q("USE taxi")
spark.read.options(header=True, inferSchema=True).csv(
    str(Path.home() / "spark-masterclass/data/nyc-taxi/taxi_zone_lookup.csv")).createOrReplaceTempView("zl")


def folders(t):
    return sorted(p.name for p in (T / t).iterdir() if p.is_dir())


print("== 1. a partitioned Hive table: a static and a dynamic insert")
q("""CREATE TABLE zones (id INT, zone STRING)
     PARTITIONED BY (borough STRING)
     STORED AS PARQUET""")
q("INSERT INTO zones PARTITION (borough = 'EWR') SELECT LocationID, Zone FROM zl WHERE Borough = 'EWR'")
q("""INSERT INTO zones PARTITION (borough)
     SELECT LocationID, Zone, Borough FROM zl WHERE Borough IN ('Bronx', 'Queens')""")
q("SHOW PARTITIONS zones")
q("SELECT borough, count(*) AS zones FROM zones GROUP BY borough ORDER BY borough")
print("  the partition value is in the folder name, not in the files:")
pq = sorted((T / "zones" / "borough=EWR").glob("*.parquet"))[0]
print("   ", [f.name for f in spark.read.parquet(str(pq)).schema.fields])
loc = describe(spark, "zones PARTITION (borough = 'Bronx')", ["Location"])["Location"]
print("  DESCRIBE ... PARTITION (borough = 'Bronx') Location:", loc.split("/warehouse/")[1])

print("\n== 2. another job writes a folder: the metastore does not know it")
shutil.copytree(T / "zones" / "borough=Bronx", T / "zones" / "borough=Bronx2")
(T / "zones" / "borough=Manhattan").mkdir()
spark.read.parquet(str(T / "zones" / "borough=Queens")).limit(3).write.mode("overwrite").parquet(
    str(T / "zones" / "borough=Manhattan"))
print("  folders:", folders("zones"))
q("SHOW PARTITIONS zones")
q("SELECT count(*) AS rows FROM zones")
q("MSCK REPAIR TABLE zones")
q("SHOW PARTITIONS zones")
q("SELECT count(*) AS rows FROM zones")

print("== 3. a folder goes away: DROP or SYNC PARTITIONS")
shutil.rmtree(T / "zones" / "borough=Bronx2")
print("  folders:", folders("zones"))
q("SELECT count(*) AS rows FROM zones")
q("REFRESH TABLE zones")
q("SELECT count(*) AS rows FROM zones")
q("MSCK REPAIR TABLE zones")
q("SHOW PARTITIONS zones")
q("MSCK REPAIR TABLE zones SYNC PARTITIONS")
q("SHOW PARTITIONS zones")

print("== 4. ALTER TABLE ... RECOVER PARTITIONS is the Spark name for ADD PARTITIONS")
shutil.copytree(T / "zones" / "borough=Queens", T / "zones" / "borough=Brooklyn")
q("ALTER TABLE zones RECOVER PARTITIONS")
q("SHOW PARTITIONS zones")

print("== 5. partition DDL: a partition outside the warehouse, and DROP on a managed table")
outside = W / "landing" / "si"
spark.sql("SELECT LocationID AS id, Zone AS zone FROM zl WHERE Borough = 'Staten Island'") \
    .write.parquet(str(outside))
q(f"ALTER TABLE zones ADD PARTITION (borough = 'Staten Island') LOCATION '{outside}'")
q("SELECT borough, count(*) AS zones FROM zones GROUP BY borough ORDER BY borough")
q("ALTER TABLE zones DROP PARTITION (borough = 'Brooklyn')")
q("ALTER TABLE zones DROP PARTITION (borough = 'Staten Island')")
print("  folders in the table:", folders("zones"))
print("  the folder outside the warehouse:", "exists" if outside.exists() else "gone")

print("\n== 6. INSERT OVERWRITE on a Hive table replaces only the partitions in the data")
q("""CREATE TABLE zones_txt (id INT, zone STRING) PARTITIONED BY (borough STRING)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE""")
FILL = "SELECT LocationID, Zone, Borough FROM zl WHERE Borough IN ('Bronx', 'Queens', 'EWR')"
q(f"INSERT OVERWRITE TABLE zones_txt PARTITION (borough) {FILL}", show=False)
q("SET hive.exec.dynamic.partition.mode = nonstrict")
for t in ["zones_txt", "zones"]:
    spark.sql(f"INSERT OVERWRITE TABLE {t} PARTITION (borough) {FILL}")
q("SET spark.sql.sources.partitionOverwriteMode")
for t in ["zones_txt", "zones"]:
    before = folders(t)
    q(f"""INSERT OVERWRITE TABLE {t} PARTITION (borough)
         SELECT LocationID, upper(Zone), Borough FROM zl WHERE Borough = 'EWR'""", show=False)
    print(f"  {t:<10} before {before}")
    print(f"  {'':<10} after  {folders(t)}")

print("\n== 7. a partitioned data source table also keeps its partitions in the metastore")
q("SET spark.sql.hive.manageFilesourcePartitions")
q("CREATE TABLE zones_ds (id INT, zone STRING, borough STRING) USING parquet PARTITIONED BY (borough)")
q("INSERT INTO zones_ds SELECT LocationID, Zone, Borough FROM zl WHERE Borough IN ('Bronx', 'EWR')")
print(" ", describe(spark, "zones_ds", ["Provider", "Partition Provider"]))
q("SHOW PARTITIONS zones_ds")
before = folders("zones_ds")
q("""INSERT OVERWRITE TABLE zones_ds
     SELECT LocationID, upper(Zone), Borough FROM zl WHERE Borough = 'EWR'""", show=False)
print(f"  zones_ds (data source table, STATIC) before {before}")
print(f"  {'':<34} after  {folders('zones_ds')}")
