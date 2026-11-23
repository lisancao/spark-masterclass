"""Chapter 84: what a view does when its table changes, in each WITH SCHEMA mode.

Run from code/:  uv run python ch84/view_schema.py > ch84/output/view_schema.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work  # noqa: E402
from ddl import runner  # noqa: E402

W = work("ch84-views")
spark = session("ch84-views", **{"spark.sql.warehouse.dir": str(W / "warehouse")})
q = runner(spark)

MODES = {"v_binding": "WITH SCHEMA BINDING", "v_comp": "", "v_type": "WITH SCHEMA TYPE EVOLUTION",
         "v_evo": "WITH SCHEMA EVOLUTION"}

print("== 1. one table, four views")
q("CREATE TABLE zones (id INT, borough STRING) USING parquet")
q("INSERT INTO zones VALUES (1, 'EWR'), (2, 'Queens')")
for v, mode in MODES.items():
    q(f"CREATE VIEW {v} {mode} AS SELECT * FROM zones")
for v in MODES:
    d = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {v}").collect()}
    print(f"  {v:<10} View Schema Mode = {d['View Schema Mode']}")


def probe(label):
    print(f"  -- {label}")
    for v in MODES:
        try:
            df = spark.table(v)
            cols = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields)
            rows = [tuple(r) for r in df.orderBy(df.columns[0]).limit(1).collect()]
            print(f"  {v:<10} ok    {cols:<40} first row {rows}")
        except Exception as e:  # noqa: BLE001
            cls = str(e).split("]")[0].lstrip("[")
            print(f"  {v:<10} error {cls}")


def replace_table(ddl, rows):
    spark.sql("DROP TABLE zones")
    spark.sql(f"CREATE TABLE zones ({ddl}) USING parquet")
    spark.sql(f"INSERT INTO zones VALUES {rows}")
    print(f"> zones is now ({ddl})")


print("\n== 2. the table changes, the views are read again")
probe("no change")
replace_table("id BIGINT, borough STRING", "(1, 'EWR'), (2, 'Queens')")
probe("id INT -> BIGINT (a widening)")
replace_table("id STRING, borough STRING", "('1', 'EWR'), ('x2', 'Queens')")
probe("id INT -> STRING, with a value that is not a number")
replace_table("id INT, borough STRING, zone STRING", "(1, 'EWR', 'Newark Airport'), (2, 'Queens', 'Jamaica Bay')")
probe("a column is added")
replace_table("id INT", "(1), (2)")
probe("a column is dropped")

print("\n== 3. what EVOLUTION wrote back to the catalog")
for v in ["v_comp", "v_evo"]:
    d = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {v}").collect()}
    print(f"  {v:<7} View Query Output Columns = {d['View Query Output Columns']}")

print("\n== 4. ALTER VIEW ... WITH SCHEMA changes the mode, not the text")
q("ALTER VIEW v_comp WITH SCHEMA EVOLUTION")
probe("after ALTER VIEW v_comp WITH SCHEMA EVOLUTION")

print("\n== 5. a column list makes EVOLUTION act as TYPE EVOLUTION")
replace_table("id INT, borough STRING", "(1, 'EWR')")
q("CREATE VIEW v_list (zone_id, zone_borough) WITH SCHEMA EVOLUTION AS SELECT * FROM zones")
d = {r.col_name: r.data_type for r in spark.sql("DESCRIBE TABLE EXTENDED v_list").collect()}
print("  v_list View Schema Mode =", d["View Schema Mode"])
replace_table("id INT, borough STRING, zone STRING", "(1, 'EWR', 'Newark Airport')")
print("  v_list columns after a column is added:", spark.table("v_list").columns)
q("CREATE TEMPORARY VIEW t_v WITH SCHEMA EVOLUTION AS SELECT * FROM zones")
q("SET spark.sql.legacy.viewSchemaCompensation")
