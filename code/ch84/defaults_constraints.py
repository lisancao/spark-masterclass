"""Chapter 84: DEFAULT column values, NOT NULL, and what the session catalog refuses
(generated columns, identity columns, CHECK and key constraints).

Run from code/:  uv run python ch84/defaults_constraints.py > ch84/output/defaults_constraints.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work, DATA  # noqa: E402
from ddl import runner, files  # noqa: E402

W = work("ch84-defaults")
WH = W / "warehouse"
ICE = DATA / "jars" / "iceberg-spark-runtime-4.3_2.13-b1eab64.jar"   # unreleased build, see notes
spark = session("ch84-defaults", **{
    "spark.sql.warehouse.dir": str(WH),
    "spark.jars": str(ICE),
    "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake.type": "hadoop",
    "spark.sql.catalog.lake.warehouse": str(W / "lake")})
q = runner(spark)

print("== 1. DEFAULT values in CREATE TABLE")
q("""CREATE TABLE fares (
       trip_id BIGINT,
       fare DECIMAL(10,2),
       currency STRING DEFAULT 'USD',
       source STRING DEFAULT 'meter',
       loaded DATE DEFAULT current_date())
     USING parquet""")
q("INSERT INTO fares (trip_id, fare) VALUES (1, 12.50)")
q("INSERT INTO fares VALUES (2, 8.00, DEFAULT, 'app', DEFAULT)")
q("INSERT INTO fares VALUES (3, 30.25)")
q("SELECT trip_id, fare, currency, source, loaded = current_date() AS loaded_today FROM fares ORDER BY trip_id")

print("== 2. ADD COLUMNS with a DEFAULT: old files stay, old rows read the default")
files(WH / "fares", "files before", "*.parquet")
q("ALTER TABLE fares ADD COLUMNS (tip DECIMAL(10,2) DEFAULT 0.00)")
files(WH / "fares", "files after", "*.parquet")
q("SELECT trip_id, fare, tip FROM fares ORDER BY trip_id")

print("== 3. SET DEFAULT changes new rows only. DROP DEFAULT goes back to NULL")
q("ALTER TABLE fares ALTER COLUMN currency SET DEFAULT 'CAD'")
q("INSERT INTO fares (trip_id, fare) VALUES (4, 5.00)")
q("ALTER TABLE fares ALTER COLUMN source DROP DEFAULT")
q("INSERT INTO fares (trip_id, fare) VALUES (5, 6.00)")
q("SELECT trip_id, currency, source FROM fares ORDER BY trip_id")
q("DESCRIBE TABLE fares", n=40)

print("== 4. Which defaults Spark accepts")
q("CREATE TABLE d_bad (id INT, r DOUBLE DEFAULT rand()) USING parquet")
q("CREATE TABLE d_sub (id INT, n INT DEFAULT (SELECT 1)) USING parquet")
q("SET spark.sql.defaultColumn.allowedProviders")
q("CREATE TABLE d_csv (id INT, c STRING DEFAULT 'x') USING csv")
q("CREATE TABLE d_text (value STRING DEFAULT 'x') USING text")

print("== 5. NOT NULL on a file source table")
q("CREATE TABLE nn_off (id INT NOT NULL, v INT) USING parquet")
q("INSERT INTO nn_off VALUES (NULL, 1)")
q("SELECT * FROM nn_off")
print(" ", [(f.name, f.nullable) for f in spark.table("nn_off").schema.fields])
q("SET spark.sql.fileSource.insert.enforceNotNull = true")
q("CREATE TABLE nn_on (id INT NOT NULL, v INT) USING parquet")
print(" ", [(f.name, f.nullable) for f in spark.table("nn_on").schema.fields])
q("INSERT INTO nn_on VALUES (NULL, 1)")
q("INSERT INTO nn_on VALUES (7, 1)")
q("SELECT * FROM nn_on")
q("SET spark.sql.fileSource.insert.enforceNotNull = false")

print("== 6. Generated columns, identity columns and constraints: the parser accepts them")
stmts = {
    "generated": "CREATE TABLE {t} (id INT, d INT GENERATED ALWAYS AS (id * 2)) {u}",
    "identity": "CREATE TABLE {t} (id BIGINT GENERATED ALWAYS AS IDENTITY, v INT) {u}",
    "check": "CREATE TABLE {t} (id INT, CONSTRAINT positive CHECK (id > 0)) {u}",
    "primary key": "CREATE TABLE {t} (id INT, v INT, CONSTRAINT pk PRIMARY KEY (id)) {u}",
}
for cat, t, u in [("spark_catalog (parquet)", "t_s", "USING parquet"), ("lake (Iceberg)", "lake.db.t_i", "")]:
    if cat.startswith("lake"):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.db")
    print(f"  -- catalog: {cat}")
    for name, s in stmts.items():
        try:
            spark.sql(s.format(t=t, u=u))
            print(f"  {name:<12} created")
            spark.sql(f"DROP TABLE {t}")
        except Exception as e:  # noqa: BLE001
            msg = str(e).split("does not support ")[-1].split(".")[0]
            print(f"  {name:<12} {type(e).__name__}: does not support {msg}")
q("ALTER TABLE fares ADD CONSTRAINT positive CHECK (fare > 0)")

print("\n== 7. NOT NULL and DEFAULT on the Iceberg table (format version 2)")
q("CREATE TABLE lake.db.nn (id INT NOT NULL, v INT)")
q("INSERT INTO lake.db.nn VALUES (NULL, 1)")
q("CREATE TABLE lake.db.dflt (id INT, c STRING DEFAULT 'x')", err_lines=3)
