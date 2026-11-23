"""Chapter 84: the forms of INSERT, the plan command that each one becomes, and the forms that
need a V2 table (REPLACE WHERE, REPLACE ON, REPLACE USING, WITH SCHEMA EVOLUTION).

Run from code/:  uv run python ch84/insert_forms.py > ch84/output/insert_forms.txt
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work, DATA  # noqa: E402
from ddl import runner, error_text  # noqa: E402

W = work("ch84-insert")
ICE = DATA / "jars" / "iceberg-spark-runtime-4.3_2.13-b1eab64.jar"
spark = session("ch84-insert", **{
    "spark.sql.warehouse.dir": str(W / "warehouse"),
    "spark.jars": str(ICE),
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake.type": "hadoop",
    "spark.sql.catalog.lake.warehouse": str(W / "lake")})
q = runner(spark)


def command(sql):
    """The first node of the physical plan of a statement (the command that does the write)."""
    try:
        text = spark.sql("EXPLAIN " + sql).first()[0]
    except Exception as e:  # noqa: BLE001
        return "error: " + str(e).split("]")[0].lstrip("[")
    if text.startswith("Error"):          # EXPLAIN hides the message: run it to get the error class
        try:
            spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            m = re.search(r"\[([A-Z_.]+)\]", str(e))
            text = str(e)
            if m and "does not support" in text:
                return "error: does not support"
            return "error: " + (m.group(1) if m else type(e).__name__)
    line = [l for l in text.splitlines() if l.strip() and not l.startswith("==")][0]
    words = re.sub(r"^[\s+\-*()0-9]*", "", line).split(" ")
    if words[0] == "Execute":
        words = words[1:]
    return words[0].split("[")[0]


def rows(t):
    return [tuple(r) for r in spark.table(t).orderBy(*spark.table(t).columns).collect()]


q("CREATE TABLE zones (id INT, zone STRING, borough STRING) USING parquet PARTITIONED BY (borough)")
q("CREATE TABLE lake.db.zones (id INT, zone STRING, borough STRING) PARTITIONED BY (borough)")

print("== 1. VALUES, a column list, and BY NAME")
for t in ["zones", "lake.db.zones"]:
    spark.sql(f"INSERT INTO {t} VALUES (1, 'Newark Airport', 'EWR')")
    spark.sql(f"INSERT INTO {t} (zone, id, borough) VALUES ('Jamaica Bay', 2, 'Queens')")
    spark.sql(f"INSERT INTO {t} BY NAME SELECT 'Bronx' AS borough, 3 AS id, 'Allerton' AS zone")
    print(f"  {t:<14} {rows(t)}")
q("INSERT INTO zones SELECT 4, 'Bronx', 'Bedford Park'")
q("INSERT INTO zones BY NAME SELECT 5 AS id, 'Bronx' AS borough")
q("SELECT * FROM zones WHERE id >= 4 ORDER BY id")
q("INSERT INTO zones BY NAME SELECT 4 AS id, 'Bronx' AS boro, 'Belmont' AS zone")

spark.sql("CREATE TEMPORARY VIEW src AS SELECT * FROM VALUES (5, 'Belmont', 'Bronx') AS s(id, zone, borough)")
print("\n== 2. the command in the plan, V1 table against V2 table")
forms = [
    ("INSERT INTO t VALUES ...", "INSERT INTO {t} VALUES (5, 'Belmont', 'Bronx')"),
    ("INSERT INTO t BY NAME SELECT", "INSERT INTO {t} BY NAME SELECT 5 AS id, 'Bronx' AS borough, 'x' AS zone"),
    ("INSERT OVERWRITE t SELECT", "INSERT OVERWRITE {t} SELECT * FROM src"),
    ("INSERT OVERWRITE t PARTITION (...) SELECT",
     "INSERT OVERWRITE {t} PARTITION (borough = 'Bronx') SELECT id, zone FROM src"),
    ("INSERT INTO t REPLACE WHERE ...", "INSERT INTO {t} REPLACE WHERE borough = 'Bronx' SELECT * FROM src"),
    ("INSERT INTO t REPLACE USING (id)", "INSERT INTO {t} REPLACE USING (id) SELECT * FROM src"),
    ("INSERT INTO t AS x REPLACE ON ...", "INSERT INTO {t} AS x REPLACE ON x.id = src.id SELECT * FROM src"),
]
print(f"  {'statement':<42} {'session catalog (parquet)':<44} V2 catalog (Iceberg)")
for label, s in forms:
    print(f"  {label:<42} {command(s.format(t='zones')):<44} {command(s.format(t='lake.db.zones'))}")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
s = forms[2][1]
print(f"  {'INSERT OVERWRITE t SELECT (dynamic mode)':<42} {command(s.format(t='zones')):<44} "
      f"{command(s.format(t='lake.db.zones'))}")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "STATIC")

print("\n== 3. REPLACE WHERE on the V2 table: delete the rows that match, insert the new rows")
q("""INSERT INTO lake.db.zones REPLACE WHERE borough = 'Bronx'
     VALUES (3, 'Allerton/Pelham Gardens', 'Bronx'), (31, 'Bronx Park', 'Bronx')""")
print("  ", rows("lake.db.zones"))
q("INSERT INTO lake.db.zones REPLACE WHERE borough = 'Bronx' VALUES (9, 'Astoria', 'Queens')")

print("\n== 4. REPLACE USING (id): replace the rows whose id is in the new data")
q("INSERT INTO lake.db.zones REPLACE USING (id) VALUES (2, 'Jamaica Bay (fixed)', 'Queens'), (7, 'Astoria', 'Queens')")
print("  ", rows("lake.db.zones"))

print("\n== 5. INSERT WITH SCHEMA EVOLUTION: a new column in the data")
try:
    spark.sql("INSERT INTO lake.db.zones BY NAME SELECT 8 AS id, 'Queens' AS borough, 'Auburndale' AS zone, "
              "'Boro Zone' AS service_zone")
except Exception as e:  # noqa: BLE001
    print("  without the clause:")
    print(error_text(e, 88, 2))
for t in ["lake.db.zones", "zones"]:
    q(f"""INSERT WITH SCHEMA EVOLUTION INTO {t} BY NAME
         SELECT 8 AS id, 'Queens' AS borough, 'Auburndale' AS zone, 'Boro Zone' AS service_zone""")
    print("  columns of", t, ":", spark.table(t).columns)
