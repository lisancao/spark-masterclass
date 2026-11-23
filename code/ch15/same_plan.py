"""Chapter 15: a SQL statement and a DataFrame call reach the same plan, also for commands.

Section 1 compares a query in both APIs at each phase. Section 2 runs CREATE TABLE AS SELECT
and DataFrameWriter.saveAsTable, and reads the command node of each from the REST API.
Section 3 compares the Catalog API and the SQL commands. Classic only (_jdf, the REST API).

Run from code/:  uv run python ch15/same_plan.py > ch15/output/same_plan.txt
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, ui_api, TRIPS, MONTH_START, MONTH_END

W = work("ch15-same")
spark = session("ch15-same", **{"spark.sql.warehouse.dir": str(W / "warehouse")})
spark.sql("CREATE NAMESPACE taxi")
spark.read.parquet(str(TRIPS)).write.saveAsTable("taxi.trips")


def tree(df, phase):
    qe = df._jdf.queryExecution()
    p = {"parsed": qe.logical(), "analyzed": qe.analyzed(), "optimized": qe.optimizedPlan()}[phase]
    return [re.sub(r"^[\s:+|-]*", "", line).split(" ")[0].replace("'", "")
            for line in p.treeString().splitlines()]


print("== 1. one query, two APIs, compared at each phase")
sql = spark.sql(f"""SELECT PULocationID, count(*) AS trips FROM taxi.trips
                    WHERE tpep_pickup_datetime >= '{MONTH_START}' GROUP BY PULocationID""")
dfa = (spark.table("taxi.trips").where(F.col("tpep_pickup_datetime") >= MONTH_START)
       .groupBy("PULocationID").agg(F.count("*").alias("trips")))
for phase in ["parsed", "analyzed", "optimized"]:
    a, b = tree(sql, phase), tree(dfa, phase)
    print(f"  {phase:<9} same node names: {a == b}")
    if phase == "parsed":
        for x, y in zip(a + [""] * (len(b) - len(a)), b + [""] * (len(a) - len(b))):
            print(f"      {x:<36} {y}")
print("  sameSemantics (analyzed):", sql.sameSemantics(dfa))
print("  sameResult (optimized):  ",
      sql._jdf.queryExecution().optimizedPlan().sameResult(dfa._jdf.queryExecution().optimizedPlan()))

print("\n== 2. a command in SQL and in the DataFrame API")


seen = {e["id"] for e in ui_api("sql")}


def last_command():
    """The command nodes of the SQL executions that ran since the last call."""
    out = []
    for e in sorted(ui_api("sql?details=true&planDescription=true"), key=lambda e: e["id"]):
        if e["id"] not in seen:
            seen.add(e["id"])
            out += re.findall(r"^\(\d+\) Execute (\w+)", e["planDescription"], re.M)
    return ", ".join(out)


spark.sql(f"""CREATE TABLE taxi.june_sql USING parquet AS
              SELECT * FROM taxi.trips WHERE tpep_pickup_datetime < '{MONTH_END}'""")
print("  CREATE TABLE ... AS SELECT  ->", last_command())
(spark.table("taxi.trips").where(F.col("tpep_pickup_datetime") < MONTH_END)
     .write.format("parquet").saveAsTable("taxi.june_df"))
print("  df.write.saveAsTable(...)   ->", last_command())
spark.sql("INSERT INTO taxi.june_sql SELECT * FROM taxi.june_sql LIMIT 1")
print("  INSERT INTO ... SELECT      ->", last_command())
spark.table("taxi.june_sql").limit(1).write.insertInto("taxi.june_sql")
print("  df.write.insertInto(...)    ->", last_command())

print("\n== 3. the Catalog API and the SQL commands read one catalog")
print("  SHOW TABLES IN taxi:          ",
      sorted(r.tableName for r in spark.sql("SHOW TABLES IN taxi").collect()))
print("  spark.catalog.listTables:     ",
      sorted(t.name for t in spark.catalog.listTables("taxi")))
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
spark.catalog.createTable("taxi.notes", source="parquet", schema=StructType(
    [StructField("id", IntegerType()), StructField("note", StringType())]))
print("  after spark.catalog.createTable('taxi.notes'):")
spark.sql("SHOW CREATE TABLE taxi.notes").show(truncate=False)
print("  spark.catalog.tableExists('taxi.notes'):", spark.catalog.tableExists("taxi.notes"))
spark.sql("DROP TABLE taxi.notes")
print("  after DROP TABLE in SQL:                ", spark.catalog.tableExists("taxi.notes"))
spark.stop()
