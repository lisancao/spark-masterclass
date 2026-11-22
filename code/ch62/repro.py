"""Chapter 62, Step 2: the minimal reproduction of SPARK-60206. No files, two tiny tables, one metric view,
one regular view as the control, and the expected answer printed next to the real one.

Run from code/:  uv run python ch62/repro.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mv import session, metric_view_sql, answer

spark = session("ch62-repro")
spark.sql("CREATE TABLE default.sales USING parquet AS "
          "SELECT * FROM VALUES ('east', 10), ('west', 20) AS t(region, amount)")
spark.sql("CREATE DATABASE other")
spark.sql("CREATE TABLE other.sales USING parquet AS SELECT * FROM VALUES ('east', 999) AS t(region, amount)")

spark.sql("USE default")
spark.sql(metric_view_sql("default.mv", "sales"))                       # source: sales (unqualified)
spark.sql("CREATE VIEW default.v AS SELECT region, sum(amount) AS total FROM sales GROUP BY region")

mv = "SELECT region, measure(total) FROM default.mv GROUP BY region ORDER BY region"
v = "SELECT region, total FROM default.v ORDER BY region"
print(f"Spark {spark.version}{'  (patched classes first on the class path)' if spark.conf.get('spark.driver.extraClassPath', '') else ''}")
print("expected everywhere: (east, 10), (west, 20)\n")
for db in ["default", "other"]:
    spark.sql(f"USE {db}")
    print(f"USE {db:<8} metric view: {answer(spark, mv):<24} regular view: {answer(spark, v)}")
