"""Chapter 45: schema evolution and partition evolution on lake.nyc.trips (run tables.py first).

The May file is the second month of data. Get it once with:
  curl -fsSL -o $BOOK_DATA/nyc-taxi/yellow-h1/yellow_tripdata_2026-05.parquet \
       https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-05.parquet
Run from code/:  uv run python ch45/evolve.py
"""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow.parquet as pq
from common.book import TAXI, ui_api
from ice import WORK, ice_session

MAY = TAXI / "yellow-h1" / "yellow_tripdata_2026-05.parquet"
TABLE = WORK / "hadoop" / "nyc" / "trips"
spark = ice_session("ch45-evolve", {"lake": "hadoop"})


def data_files():
    return {p.relative_to(TABLE / "data") for p in (TABLE / "data").rglob("*.parquet")}


def current_metadata():
    v = (TABLE / "metadata" / "version-hint.text").read_text().strip()
    return v, json.loads((TABLE / "metadata" / f"v{v}.metadata.json").read_text())


before = data_files()
print("== Schema evolution")
for sql in ["ALTER TABLE lake.nyc.trips ADD COLUMN tip_pct DOUBLE AFTER tip_amount",
            "ALTER TABLE lake.nyc.trips RENAME COLUMN fare_amount TO fare",
            "ALTER TABLE lake.nyc.trips ALTER COLUMN PULocationID TYPE BIGINT",
            "ALTER TABLE lake.nyc.trips DROP COLUMN payment_type"]:
    spark.sql(sql)
    v, m = current_metadata()
    print(f"v{v}.metadata.json  current-schema-id {m['current-schema-id']}  <- {sql[26:]}")

print(f"\ndata files before: {len(before)}, after: {len(data_files())}, "
      f"same files: {before == data_files()}")
v, m = current_metadata()
print(f"v{v}.metadata.json keeps {len(m['schemas'])} schemas. Field IDs in the current one:")
print("  " + ", ".join(f"{f['id']}:{f['name']}:{f['type']}" for f in m["schemas"][-1]["fields"]))

f = sorted(TABLE.glob("data/*/*.parquet"))[0]
pf = pq.read_schema(f)
print(f"\nan old Parquet file still says (name, field_id):")
print("  " + ", ".join(f"{n}:{pf.field(n).metadata[b'PARQUET:field_id'].decode()}"
                       for n in ("fare_amount", "PULocationID", "payment_type")))
print(f"  PULocationID physical type in the file: {pf.field('PULocationID').type}")
spark.sql("SELECT tpep_pickup_datetime, fare, tip_pct, PULocationID FROM lake.nyc.trips LIMIT 2").show()

print("== Partition evolution: daily partitions for June, monthly from now on")
spark.sql("ALTER TABLE lake.nyc.trips REPLACE PARTITION FIELD tpep_pickup_datetime_day "
          "WITH months(tpep_pickup_datetime)")
v, m = current_metadata()
for spec in m["partition-specs"]:
    print(f"  spec {spec['spec-id']}: " +
          ", ".join(f"{x['transform']}({x['source-id']}) as {x['name']}" for x in spec["fields"]))
print(f"  default-spec-id {m['default-spec-id']}")

may = (spark.read.parquet(str(MAY))
       .where("tpep_pickup_datetime >= '2026-05-01' AND tpep_pickup_datetime < '2026-06-01'")
       .withColumnRenamed("fare_amount", "fare")
       .selectExpr(*[c if c != "tip_pct" else "round(tip_amount / nullif(fare, 0), 3) AS tip_pct"
                     for c in spark.table("lake.nyc.trips").columns]))
may.writeTo("lake.nyc.trips").append()
print("\nfiles per partition spec (lake.nyc.trips.files):")
spark.sql("SELECT spec_id, count(*) AS files, sum(record_count) AS rows FROM lake.nyc.trips.files "
          "GROUP BY spec_id ORDER BY spec_id").show()
print("data/ folders:", sorted(p.name for p in (TABLE / "data").iterdir())[-3:])

print("\n== One query, two layouts")
for day in ("2026-05-15", "2026-06-15"):
    spark.sparkContext.setJobDescription(day)
    n = spark.sql(f"SELECT count(*) FROM lake.nyc.trips WHERE tpep_pickup_datetime >= '{day}' "
                  f"AND tpep_pickup_datetime < date_add('{day}', 1)").first()[0]
    q = ui_api("sql?details=true")[-1]
    mt = {x["name"]: x["value"] for node in q["nodes"] if node["nodeName"].startswith("BatchScan")
          for x in node["metrics"]}
    print(f"{day}: {n:>7,} trips | manifests: scanned {mt['number of scanned data manifests']}, "
          f"skipped {mt['number of skipped data manifests']} | data files: read "
          f"{mt['number of result data files']}, skipped {mt['number of skipped data files']}")
