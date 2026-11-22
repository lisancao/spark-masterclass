"""Chapter 31: create a partitioned Iceberg table from the June trips, and look at the files.

Run from code/:  uv run python ch31/tables.py
"""
import json
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import TRIPS, MONTH_START, MONTH_END, plan, ui_api
from ice import WORK, ice_session, tidy, tree

shutil.rmtree(WORK / "hadoop" / "nyc" / "trips", ignore_errors=True)
spark = ice_session("ch31-tables", {"lake": "hadoop"})
TABLE = WORK / "hadoop" / "nyc" / "trips"


def metadata(version):
    return json.loads((TABLE / "metadata" / f"v{version}.metadata.json").read_text())


print("== CREATE TABLE")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.nyc")
spark.sql("""
    CREATE TABLE lake.nyc.trips (
        VendorID INT, tpep_pickup_datetime TIMESTAMP_NTZ, tpep_dropoff_datetime TIMESTAMP_NTZ,
        passenger_count BIGINT, trip_distance DOUBLE, PULocationID INT, DOLocationID INT,
        payment_type BIGINT, fare_amount DOUBLE, tip_amount DOUBLE, total_amount DOUBLE)
    USING iceberg
    PARTITIONED BY (days(tpep_pickup_datetime))""")
print(tree(TABLE))
m = metadata(1)
print(f"\nv1.metadata.json: format-version {m['format-version']}, "
      f"current-snapshot-id {m.get('current-snapshot-id')}, snapshots {len(m['snapshots'])}")
print("  schema:", ", ".join(f"{f['id']}:{f['name']}:{f['type']}" for f in m["schemas"][0]["fields"][:4]),
      "...")
print("  partition spec:", json.dumps(m["partition-specs"]))

print("\n== INSERT the June trips")
june = (spark.read.parquet(str(TRIPS))
        .where(f"tpep_pickup_datetime >= '{MONTH_START}' AND tpep_pickup_datetime < '{MONTH_END}'"))
june.createOrReplaceTempView("june")
cols = ", ".join(spark.table("lake.nyc.trips").columns)
spark.sql(f"INSERT INTO lake.nyc.trips SELECT {cols} FROM june")
print("rows:", f"{spark.table('lake.nyc.trips').count():,}")
days = sorted(p.name for p in (TABLE / "data").iterdir())
print(f"data/: {len(days)} folders, {days[0]} ... {days[-1]}")
print(f"data files: {sum(1 for _ in (TABLE / 'data').rglob('*.parquet'))}")
print("metadata/:")
print(tree(TABLE / "metadata"))
m = metadata(2)
snap = m["snapshots"][-1]
print(f"\nv2.metadata.json: current-snapshot-id {m['current-snapshot-id']}")
print(f"  manifest-list: {tidy(snap['manifest-list']).split('/')[-1]}")
print("  summary:", {k: snap["summary"][k] for k in
                     ("operation", "added-data-files", "added-records", "total-files-size")})

print("\n== Hidden partitioning: filter on the timestamp, not on a partition column")
spark.sparkContext.setJobDescription("one day")
one_day = spark.sql("""
    SELECT count(*) AS trips, round(avg(total_amount), 2) AS avg_total FROM lake.nyc.trips
    WHERE tpep_pickup_datetime >= '2026-06-15' AND tpep_pickup_datetime < '2026-06-16'""")
scan = [l.strip(" +-") for l in plan(one_day, "simple").splitlines() if "BatchScan" in l]
print(scan[0])
print(one_day.collect()[0].asDict())
q = ui_api("sql?details=true")[-1]
for node in q["nodes"]:
    if node["nodeName"].startswith("BatchScan"):
        for mtr in node["metrics"]:
            if mtr["name"] in ("total data manifests", "number of scanned data manifests",
                               "number of skipped data files", "number of result data files",
                               "total data file size (bytes)", "number of output rows"):
                print(f"  {mtr['name']}: {mtr['value']}")
