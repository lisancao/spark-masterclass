"""Chapter 84: the forms of CREATE TABLE on the session catalog, and what each one puts on disk.

Run from code/:  uv run python ch84/create_variants.py > ch84/output/create_variants.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work, TRIPS  # noqa: E402
from ddl import runner, files  # noqa: E402

W = work("ch84-create")
WH = W / "warehouse"
spark = session("ch84-create", **{"spark.sql.warehouse.dir": str(WH)})
q = runner(spark)

spark.read.parquet(str(TRIPS)).createOrReplaceTempView("trips_raw")
q("CREATE NAMESPACE taxi")
q("USE taxi")

print("== 1. CREATE TABLE ... USING: columns, partitions, buckets, options, properties")
q("""CREATE TABLE trips_by_day (
       vendor INT COMMENT 'VendorID from the source',
       pu_zone INT,
       fare DECIMAL(10,2),
       day DATE)
     USING parquet
     PARTITIONED BY (day)
     CLUSTERED BY (pu_zone) INTO 4 BUCKETS
     OPTIONS ('compression' = 'zstd')
     COMMENT 'one row per trip, partitioned by pickup day'
     TBLPROPERTIES ('owner.team' = 'ops')""")
d = {r.col_name: r.data_type for r in spark.sql("DESCRIBE TABLE EXTENDED trips_by_day").collect()}
for k in ["Type", "Provider", "Num Buckets", "Bucket Columns", "Comment", "Table Properties",
          "Storage Properties", "Partition Provider"]:
    print(f"  {k:<19} {d.get(k)}")

q("""INSERT INTO trips_by_day
     SELECT VendorID, PULocationID, CAST(fare_amount AS DECIMAL(10,2)), to_date(tpep_pickup_datetime)
     FROM trips_raw
     WHERE tpep_pickup_datetime >= '2026-06-01' AND tpep_pickup_datetime < '2026-06-03'""")
files(WH / "taxi.db" / "trips_by_day", "on disk: one folder per day, one file per bucket per task")

print("\n== 2. CREATE TABLE with no USING: the provider is spark.sql.sources.default")
q("SET spark.sql.sources.default", show=True)
q("CREATE TABLE notes (id INT, note STRING)")
d = {r.col_name: r.data_type for r in spark.sql("DESCRIBE TABLE EXTENDED notes").collect()}
print("  notes Provider:", d["Provider"])

print("\n== 3. CREATE TABLE ... AS SELECT (CTAS): the schema comes from the query")
q("""CREATE TABLE daily USING parquet PARTITIONED BY (day) AS
     SELECT count(*) AS trips, round(sum(fare_amount), 2) AS fares, to_date(tpep_pickup_datetime) AS day
     FROM trips_raw
     WHERE tpep_pickup_datetime >= '2026-06-01' AND tpep_pickup_datetime < '2026-06-04'
     GROUP BY 3""")
q("DESCRIBE TABLE daily")
files(WH / "taxi.db" / "daily", "on disk")
print("  a CTAS cannot also give a column list:")
q("CREATE TABLE bad (n BIGINT) USING parquet AS SELECT 1 AS n")

print("\n== 4. CREATE TABLE LIKE: the definition, no data")
q("CREATE TABLE daily_empty LIKE daily")
q("CREATE TABLE daily_json LIKE daily USING json")
for t in ["daily", "daily_empty", "daily_json"]:
    d = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {t}").collect()}
    n = spark.table(t).count()
    print(f"  {t:<12} Provider={d['Provider']:<8} rows={n:<3} partition columns: "
          f"{[c.name for c in spark.catalog.listColumns(t, 'taxi') if c.isPartition]}")

print("\n== 5. IF NOT EXISTS, and a second CREATE without it")
q("CREATE TABLE IF NOT EXISTS daily (x INT) USING parquet")
q("CREATE TABLE daily (x INT) USING parquet")

print("\n== 6. REPLACE TABLE and CREATE OR REPLACE TABLE need a V2 catalog")
q("CREATE OR REPLACE TABLE daily USING parquet AS SELECT 1 AS trips")
q("REPLACE TABLE daily (x INT) USING parquet")

print("\n== 7. EXTERNAL needs LOCATION, and LOCATION makes a table external")
q("CREATE EXTERNAL TABLE ext_nopath (x INT) USING parquet")
q(f"CREATE TABLE ext_daily USING parquet LOCATION '{WH / 'taxi.db' / 'daily'}'")
d = {r.col_name: r.data_type for r in spark.sql("DESCRIBE TABLE EXTENDED ext_daily").collect()}
print("  ext_daily Type:", d["Type"], "| Partition Provider:", d.get("Partition Provider"))
q("SELECT count(*) AS rows FROM ext_daily")
q("SHOW PARTITIONS ext_daily")
q("MSCK REPAIR TABLE ext_daily")
q("SELECT count(*) AS rows FROM ext_daily")
