"""Chapter 12: managed and external tables, CTAS, DESCRIBE, and the three kinds of view.

Run from code/:  uv run python ch12/tables_views.py > ch12/output/tables_views.txt
"""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work, tree, TRIPS, error_text

W = work("ch12-tables")
spark = session("ch12-tables", **{"spark.sql.warehouse.dir": str(W / "warehouse")})


def q(sql, n=30, truncate=False):
    print(f"> {sql}")
    df = spark.sql(sql)
    if df.columns:
        df.show(n, truncate=truncate)


def files(p, label):
    p = Path(p)
    n = sum(1 for f in p.rglob("*.parquet")) if p.exists() else 0
    print(f"  {label}: {'exists' if p.exists() else 'gone'}, {n} parquet files")


spark.read.parquet(str(TRIPS)).createOrReplaceTempView("trips_raw")
q("CREATE NAMESPACE taxi")
q("USE taxi")

print("== 1. a managed table: CTAS into the warehouse")
q("""CREATE TABLE daily USING parquet AS
     SELECT to_date(tpep_pickup_datetime) AS day, count(*) AS trips, round(sum(fare_amount), 2) AS fares
     FROM trips_raw
     WHERE tpep_pickup_datetime >= '2026-06-01' AND tpep_pickup_datetime < '2026-07-01'
     GROUP BY 1""")
q("SELECT * FROM daily ORDER BY day LIMIT 3")

print("== 2. an external table: the files stay where you put them")
ext = W / "external" / "daily_copy"
spark.table("daily").write.parquet(str(ext))
q(f"CREATE TABLE daily_ext USING parquet LOCATION '{ext}'")
q("SELECT count(*) AS days FROM daily_ext")

print("== 3. DESCRIBE TABLE EXTENDED, the rows that differ")
for t in ["daily", "daily_ext"]:
    d = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {t}").collect()}
    print(f"  {t}")
    for k in ["Catalog", "Database", "Table", "Type", "Provider", "Location"]:
        print(f"    {k:<9} {d.get(k)}")

print("\n== 4. DESCRIBE TABLE ... AS JSON (a document a program can read)")
doc = json.loads(spark.sql("DESCRIBE TABLE EXTENDED daily AS JSON").first()[0])
print("  keys:", sorted(doc)[:14])
print("  columns:", [(c["name"], c["type"]["name"]) for c in doc["columns"]])

print("\n== 5. SHOW TABLES ... AS JSON (new in 4.3)")
print(" ", spark.sql("SHOW TABLES IN taxi AS JSON").first()[0])

print("\n== 6. DROP TABLE: managed deletes the data, external keeps it")
managed_loc = W / "warehouse" / "taxi.db" / "daily"
files(managed_loc, "daily     before")
files(ext, "daily_ext before")
q("DROP TABLE daily")
q("DROP TABLE daily_ext")
files(managed_loc, "daily     after ")
files(ext, "daily_ext after ")

print("\n== 7. three kinds of view")
q("""CREATE OR REPLACE TEMPORARY VIEW june AS
     SELECT * FROM trips_raw
     WHERE tpep_pickup_datetime >= '2026-06-01' AND tpep_pickup_datetime < '2026-07-01'""")
q("""CREATE OR REPLACE GLOBAL TEMPORARY VIEW june_g AS SELECT * FROM june""")
spark.table("june").createOrReplaceGlobalTempView("june_df")   # from a DataFrame: stores the plan
q("CREATE TABLE zones_t USING parquet AS SELECT 1 AS LocationID, 'x' AS Zone")
q("CREATE OR REPLACE VIEW zone_names AS SELECT LocationID, upper(Zone) AS zone FROM zones_t")
q("SHOW VIEWS IN taxi")
q("SHOW VIEWS IN global_temp")

print("== 8. which session can see which view")
other = spark.newSession()
for name in ["june", "global_temp.june_g", "global_temp.june_df", "taxi.zone_names"]:
    try:
        other.table(name).limit(1).collect()
        print(f"  newSession() reads {name:<20} yes")
    except Exception as e:
        print(f"  newSession() reads {name:<20} no: {str(e).split(']')[0]}]")

print("\n== 9. a persistent view stores text, not data")
d = {r.col_name: r.data_type for r in spark.sql("DESCRIBE TABLE EXTENDED zone_names").collect()}
for k in ["Type", "View Text", "View Original Text", "View Schema Mode", "View Query Output Columns"]:
    print(f"  {k:<26} {d.get(k)}")
print("  a persistent view cannot use a temporary view:")
try:
    spark.sql("CREATE VIEW bad AS SELECT * FROM june")
except Exception as e:
    print(error_text(e, width=86, lines=2))

print("\n== 10. WITH SCHEMA EVOLUTION: the view follows a changed table (new in 4.0)")
q("CREATE OR REPLACE VIEW zones_all WITH SCHEMA EVOLUTION AS SELECT * FROM zones_t")
q("CREATE OR REPLACE VIEW zones_bound AS SELECT * FROM zones_t")
q("DROP TABLE zones_t")
q("CREATE TABLE zones_t USING parquet AS SELECT 1 AS LocationID, 'x' AS Zone, 'EWR' AS Borough")
q("SELECT * FROM zones_all")
q("SELECT * FROM zones_bound")
spark.stop()
