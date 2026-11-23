"""Chapter 12: catalogs, namespaces and three-part names.

The session catalog (spark_catalog) is always there. A second catalog, `h2`, is Spark's own
JDBCTableCatalog over an H2 database file, so that one session holds two catalogs.
Downloads com.h2database:h2 from Maven Central the first time.

Run from code/:  uv run python ch12/catalogs.py > ch12/output/catalogs.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work, TRIPS, ZONES

W = work("ch12-catalogs")
spark = session(
    "ch12-catalogs",
    **{"spark.jars.packages": "com.h2database:h2:2.3.232",
       "spark.sql.warehouse.dir": str(W / "warehouse"),
       "spark.sql.catalog.h2": "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
       "spark.sql.catalog.h2.url": f"jdbc:h2:file:{W}/h2db",
       "spark.sql.catalog.h2.driver": "org.h2.Driver"})


def q(sql, n=20):
    print(f"> {sql}")
    df = spark.sql(sql)
    if df.columns:          # DDL and USE return an empty result: print nothing for them
        df.show(n, truncate=False)


print("== 1. where am I")
q("SELECT current_catalog(), current_database(), current_schema()")
q("SHOW CATALOGS")

print("== 2. namespaces in the session catalog")
q("CREATE NAMESPACE IF NOT EXISTS taxi COMMENT 'NYC taxi data'")
q("SHOW NAMESPACES")
q("DESCRIBE NAMESPACE EXTENDED taxi")

print("== 3. a table, and the names that reach it")
spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES)) \
    .write.saveAsTable("taxi.zones")
q("SELECT count(*) FROM taxi.zones")
q("SELECT count(*) FROM spark_catalog.taxi.zones")
q("USE taxi")
q("SELECT count(*) FROM zones")
q("SELECT current_catalog(), current_database()")

print("== 4. a second catalog: SHOW CATALOGS lists it only after first use")
q("SHOW CATALOGS")
q("CREATE NAMESPACE IF NOT EXISTS h2.ops")
q("SHOW CATALOGS")
q("SHOW NAMESPACES IN h2")
q("""CREATE TABLE h2.ops.zone_owner (LocationID INT, owner STRING)""")
q("""INSERT INTO h2.ops.zone_owner VALUES (132, 'airport team'), (138, 'airport team'),
                                        (161, 'midtown team')""")

print("== 5. one query, two catalogs")
q("""SELECT z.Zone, o.owner
     FROM spark_catalog.taxi.zones AS z JOIN h2.ops.zone_owner AS o USING (LocationID)
     ORDER BY z.Zone""")

print("== 6. USE a catalog, and unqualified names follow it")
q("USE h2.ops")
q("SELECT current_catalog(), current_database()")
q("SHOW TABLES")
q("USE spark_catalog.taxi")
q("SHOW TABLES")

print("== 7. the Catalog API reads the same catalog")
print("  spark.catalog.currentCatalog():", spark.catalog.currentCatalog())
print("  spark.catalog.listCatalogs():  ", [c.name for c in spark.catalog.listCatalogs()])
print("  spark.catalog.listTables('taxi'):", [(t.name, t.tableType) for t in spark.catalog.listTables("taxi")])
spark.stop()
