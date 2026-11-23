"""Chapter 84: ALTER TABLE on session catalog tables, partition DDL, TRUNCATE and COMMENT ON (4.3),
with the files on disk after each change. The same column changes on an Iceberg table for contrast.

Run from code/:  uv run python ch84/alter_comment.py > ch84/output/alter_comment.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work, DATA, TRIPS  # noqa: E402
from ddl import runner, files  # noqa: E402

W = work("ch84-alter")
WH = W / "warehouse"
ICE = DATA / "jars" / "iceberg-spark-runtime-4.3_2.13-b1eab64.jar"
spark = session("ch84-alter", **{
    "spark.sql.warehouse.dir": str(WH),
    "spark.jars": str(ICE),
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake.type": "hadoop",
    "spark.sql.catalog.lake.warehouse": str(W / "lake")})
q = runner(spark)
spark.read.parquet(str(TRIPS)).createOrReplaceTempView("trips_raw")
q("CREATE NAMESPACE taxi")
q("USE taxi")
q("""CREATE TABLE daily USING parquet PARTITIONED BY (day) AS
     SELECT count(*) AS trips, round(sum(fare_amount), 2) AS fares, to_date(tpep_pickup_datetime) AS day
     FROM trips_raw
     WHERE tpep_pickup_datetime >= '2026-06-01' AND tpep_pickup_datetime < '2026-06-04'
     GROUP BY 3""")

print("== 1. RENAME TO moves the folder of a managed table")
q("CREATE TABLE zones USING parquet AS SELECT 1 AS LocationID, 'EWR' AS Borough")
files(WH / "taxi.db", "before", "*.parquet")
q("ALTER TABLE zones RENAME TO zone_lookup")
files(WH / "taxi.db", "after", "*.parquet")
q("SELECT * FROM zone_lookup")


def part_loc(t, spec):
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {t} PARTITION ({spec})").collect()
    return [r.data_type for r in rows if r.col_name == "Location"][0].split("/warehouse/")[1]


print("  a partitioned table in the in-memory catalog:")
q("CREATE TABLE p_old USING parquet PARTITIONED BY (p) AS SELECT 1 AS v, 1 AS p")
print("    partition p=1 before:", part_loc("p_old", "p=1"))
q("ALTER TABLE p_old RENAME TO p_new")
print("    partition p=1 after: ", part_loc("p_new", "p=1"))
print("    rows in p_new:", spark.table("p_new").count())
files(WH / "taxi.db" / "p_new", "p_new on disk", "*.parquet")

print("\n== 2. Column changes that the session catalog accepts, and the ones it refuses")
q("ALTER TABLE daily ADD COLUMNS (tips DOUBLE COMMENT 'sum of tip_amount')")
q("ALTER TABLE daily ALTER COLUMN fares COMMENT 'sum of fare_amount'")
q("ALTER TABLE daily RENAME COLUMN fares TO fare_total")
q("ALTER TABLE daily DROP COLUMN tips")
q("ALTER TABLE daily ALTER COLUMN trips TYPE INT")
q("ALTER TABLE daily ALTER COLUMN fares TYPE DECIMAL(12,2)")
q("ALTER TABLE daily REPLACE COLUMNS (trips BIGINT, fares DOUBLE)")

print("\n== 3. Table properties")
q("ALTER TABLE daily SET TBLPROPERTIES ('owner.team' = 'ops', 'quality' = 'raw')")
q("ALTER TABLE daily UNSET TBLPROPERTIES ('quality')")
q("SHOW TBLPROPERTIES daily")

print("== 4. Partition DDL: ADD, RENAME, DROP")
q("ALTER TABLE daily ADD PARTITION (day = '2026-06-04')")
files(WH / "taxi.db" / "daily", "after ADD PARTITION (an empty folder has no file)")
print("   folders:", sorted(p.name for p in (WH / "taxi.db" / "daily").iterdir() if p.is_dir()))
q("ALTER TABLE daily PARTITION (day = '2026-06-03') RENAME TO PARTITION (day = '2026-06-30')")
q("ALTER TABLE daily DROP PARTITION (day = '2026-06-01')")
q("SHOW PARTITIONS daily")
files(WH / "taxi.db" / "daily", "on disk", "*.parquet")
q("SELECT day, trips FROM daily ORDER BY day")

print("== 5. TRUNCATE TABLE: one partition, then all of it")
q("TRUNCATE TABLE daily PARTITION (day = '2026-06-02')")
files(WH / "taxi.db" / "daily", "after TRUNCATE ... PARTITION", "*.parquet")
q("SHOW PARTITIONS daily")
q("TRUNCATE TABLE daily")
files(WH / "taxi.db" / "daily", "after TRUNCATE", "*.parquet")
q("SHOW PARTITIONS daily")
ext = W / "external" / "zones"
spark.createDataFrame([(1, "EWR")], "id INT, borough STRING").write.parquet(str(ext))
q(f"CREATE TABLE zones_ext USING parquet LOCATION '{ext}'")
q("TRUNCATE TABLE zones_ext")

print("\n== 6. COMMENT ON (COLUMN and the list form are new in 4.3)")
q("CREATE TABLE trip_notes (id BIGINT, pickup STRUCT<zone: INT, borough: STRING>, note STRING) USING parquet")
q("COMMENT ON NAMESPACE taxi IS 'NYC taxi tables of the book'")
q("COMMENT ON TABLE trip_notes IS 'free-text notes per trip'")
q("COMMENT ON COLUMN trip_notes.note IS 'what the driver wrote'")
q("""COMMENT ON TABLE trip_notes COLUMN (
       id IS 'trip id from the source',
       pickup.borough IS 'borough of the pickup zone')""")
q("DESCRIBE TABLE trip_notes")
print("  the comment of the nested field:")
print("  ", spark.table("trip_notes").schema["pickup"].dataType["borough"].metadata)
q("COMMENT ON COLUMN trip_notes.note IS NULL")
print("  note comment now:", [r.comment for r in spark.sql("DESCRIBE TABLE trip_notes").collect() if r.col_name == "note"])
q("DESCRIBE NAMESPACE taxi")
q("CREATE VIEW notes_v AS SELECT id, note FROM trip_notes")
q("COMMENT ON COLUMN notes_v.note IS 'x'")

print("\n== 7. The same column changes on a V2 table (Iceberg)")
q("CREATE NAMESPACE lake.taxi")
q("CREATE TABLE lake.taxi.daily (trips INT, fares DOUBLE, day DATE) PARTITIONED BY (day)")
q("ALTER TABLE lake.taxi.daily RENAME COLUMN fares TO fare_total")
q("ALTER TABLE lake.taxi.daily ALTER COLUMN trips TYPE BIGINT")
q("ALTER TABLE lake.taxi.daily ADD COLUMNS (tips DOUBLE)")
q("ALTER TABLE lake.taxi.daily DROP COLUMN tips")
q("COMMENT ON COLUMN lake.taxi.daily.fare_total IS 'sum of fare_amount'")
q("DESCRIBE TABLE lake.taxi.daily")
q("ALTER TABLE lake.taxi.daily ADD PARTITION (day = '2026-06-04')")
q("INSERT INTO lake.taxi.daily VALUES (1, 2.0, DATE'2026-06-01')")
q("TRUNCATE TABLE lake.taxi.daily")
q("SELECT count(*) AS rows FROM lake.taxi.daily")
