"""Chapter 91, Steps 3-4: where SPARK-60206 starts and stops, and the evidence for the hypothesis.
Each row of the table changes one thing against the minimal reproduction.

Run from code/:  uv run python ch91/boundaries.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mv import session, metric_view_sql, answer

spark = session("ch91-boundaries")
spark.sql("CREATE TABLE default.sales USING parquet AS "
          "SELECT * FROM VALUES ('east', 10), ('west', 20) AS t(region, amount)")
spark.sql("CREATE TABLE default.regions USING parquet AS SELECT * FROM VALUES ('east'), ('west') AS t(region)")
spark.sql("CREATE DATABASE other")
spark.sql("CREATE TABLE other.sales USING parquet AS SELECT * FROM VALUES ('east', 999) AS t(region, amount)")
spark.sql("CREATE TABLE other.regions USING parquet AS SELECT * FROM VALUES ('west') AS t(region)")
spark.sql("CREATE DATABASE empty")

spark.sql("USE default")
spark.sql(metric_view_sql("mv_asset", "sales"))
spark.sql(metric_view_sql("mv_qualified", "default.sales"))
spark.sql(metric_view_sql("mv_sql", "SELECT * FROM sales"))
spark.sql(metric_view_sql("mv_filter", "default.sales", filt="region IN (SELECT region FROM regions)"))
spark.sql("CREATE VIEW v AS SELECT region, sum(amount) AS total FROM sales GROUP BY region")

def mq(name):
    return f"SELECT region, measure(total) FROM default.{name} GROUP BY region ORDER BY region"
V = "SELECT region, total FROM default.v ORDER BY region"

print("expected: (east, 10), (west, 20)\n")
print(f"{'case':<46} {'metric view':<31} regular view")
rows = [("1 USE other (has its own sales)", "other", "mv_asset", None),
        ("2 USE empty (no sales table)", "empty", "mv_asset", None),
        ("3 USE default, temp view named sales", "default", "mv_asset", "temp"),
        ("4 source qualified as default.sales, USE other", "other", "mv_qualified", None),
        ("5 SQL source SELECT * FROM sales, USE other", "other", "mv_sql", None),
        ("6 subquery in filter: FROM regions, USE other", "other", "mv_filter", None)]
for label, db, name, extra in rows:
    spark.sql(f"USE {db}")
    if extra == "temp":
        spark.sql("CREATE TEMPORARY VIEW sales AS SELECT * FROM VALUES ('north', 5) AS t(region, amount)")
    print(f"{label:<46} {answer(spark, mq(name)):<31} {answer(spark, V)}")
    if extra == "temp":
        spark.sql("DROP VIEW sales")

print("\nEvidence 1: the catalog stores the creation namespace of both views")
cat = spark._jsparkSession.sessionState().catalog()
TI = spark._jvm.org.apache.spark.sql.catalyst.TableIdentifier
for name in ["mv_asset", "v"]:
    md = cat.getTableMetadata(TI.apply(name, spark._jvm.scala.Some("default")))
    ns = md.viewCatalogAndNamespace()
    print(f"  {name:<9} type {md.tableType().name():<12} viewCatalogAndNamespace "
          f"[{', '.join(ns.apply(i) for i in range(ns.size()))}]")

print("\nEvidence 2: the relations in the analyzed plans, after USE other")
spark.sql("USE other")
for label, sql in [("metric view", mq("mv_asset")), ("regular view", V)]:
    qe = spark.sql(sql)._jdf.queryExecution()
    text = qe.analyzed().toString()
    rels = sorted({l.strip().lstrip("+- :").split(" [")[0].split("[")[0].strip()
                   for l in text.splitlines() if "Relation " in l})
    print(f"  {label:<13} {rels}")
