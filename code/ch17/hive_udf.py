"""Chapter 17: Hive UDFs in Spark. Temporary and persistent functions from a jar (CREATE FUNCTION ...
USING JAR), the wrapper that each Hive class becomes in the plan, and a session without Hive support.

Build the jar first:  ch17/hive_udf/build.sh
Run from code/:  uv run python ch17/hive_udf.py > ch17/output/hive_udf.txt
Each step runs in its own process, because the Derby metastore accepts one process at a time.
"""
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
JAR = HERE / "hive_udf" / "book-hive-udf.jar"
sys.path.insert(0, str(HERE))
from hive import work  # noqa: E402

W = work("ch17-udf")
PRELUDE = f"""
import re, sys
sys.path.insert(0, {str(HERE)!r})
from hive import hive_session, runner
JAR = {str(JAR)!r}
spark = hive_session("ch17-udf", {str(W)!r}, **EXTRA)
q = runner(spark)
def node(sql):
    plan = spark.sql("EXPLAIN EXTENDED " + sql).first()[0]
    phys = plan.split("== Physical Plan ==")[1]
    names = sorted(set(re.findall(r"(Hive[A-Za-z]+|ObjectHashAggregate)", phys)) - {{"HiveFunctionWrapper"}})
    return names
"""

STEP_1 = """
print("== 1. temporary functions from a jar")
q("CREATE DATABASE taxi")
q("CREATE TABLE taxi.zones (id INT, borough STRING, zone STRING) STORED AS PARQUET")
spark.sql("INSERT INTO taxi.zones VALUES (4, 'Manhattan', 'Alphabet City'), (7, 'Queens', 'Astoria'), "
          "(1, 'EWR', 'Newark Airport'), (24, 'Manhattan', 'Bloomingdale'), (238, 'Manhattan', 'Upper West Side North')")
q(f"CREATE TEMPORARY FUNCTION borough_code AS 'book.udf.BoroughCode' USING JAR '{JAR}'")
q(f"CREATE TEMPORARY FUNCTION initials AS 'book.udf.Initials' USING JAR '{JAR}'")
q("SELECT id, borough, borough_code(borough) AS code, zone, initials(zone) AS short FROM taxi.zones ORDER BY id")
print("  wrapper in the physical plan:")
print("    borough_code ->", node("SELECT borough_code(borough) FROM taxi.zones"))
print("    initials     ->", node("SELECT initials(zone) FROM taxi.zones"))

print("\\n== 2. a persistent function: the class name and the jar go into the metastore")
q(f"CREATE FUNCTION taxi.borough_code AS 'book.udf.BoroughCode' USING JAR '{JAR}'")
q("SHOW USER FUNCTIONS IN taxi")
rows = spark.sql("DESCRIBE FUNCTION EXTENDED taxi.borough_code").collect()
for r in rows:
    print("  " + r[0].replace(JAR, "<jar>"))

print("\\n== 3. Hive's own UDAF and UDTF classes, from the hive-exec jar that Spark ships")
q("CREATE TEMPORARY FUNCTION hive_std AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStd'")
q("CREATE TEMPORARY FUNCTION hive_stack AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFStack'")
q("SELECT borough, round(hive_std(id), 2) AS std_id, round(stddev_pop(id), 2) AS spark_std FROM taxi.zones GROUP BY borough ORDER BY borough")
q("SELECT hive_stack(2, 'Bronx', 'BX', 'Brooklyn', 'BK') AS (borough, code)")
print("  wrapper in the physical plan:")
print("    hive_std   ->", node("SELECT hive_std(id) FROM taxi.zones"))
print("    hive_stack ->", node("SELECT hive_stack(2, 'a', 'b', 'c', 'd')"))
"""

STEP_2 = """
print("== 4. a new process: the persistent function works with no CREATE in this session")
q("SELECT id, taxi.borough_code(borough) AS code FROM taxi.zones ORDER BY id")
q("SELECT initials('Upper West Side')")
print("  jars that the session added for the function:",
      [j.split("/")[-1] for j in spark.sparkContext._jsc.sc().listJars().mkString(",").split(",") if j])
"""

STEP_3 = """
print("== 5. a session without Hive support (the in-memory catalog)")
q(f"CREATE TEMPORARY FUNCTION borough_code AS 'book.udf.BoroughCode' USING JAR '{JAR}'")
q("SELECT borough_code('Queens') AS code")
q(f"CREATE FUNCTION borough_code2 AS 'book.udf.BoroughCode' USING JAR '{JAR}'")
q("SELECT borough_code2('Queens') AS code")
"""


def run(body, extra="{}", in_memory=False):
    code = f"EXTRA = {extra}\n" + PRELUDE + body
    if in_memory:      # the same helpers, but no Hive support
        code = code.replace("from hive import hive_session, runner",
                            "from hive import runner\nfrom pyspark.sql import SparkSession\n"
                            "def hive_session(name, folder, **conf):\n"
                            "    s = SparkSession.builder.master('local[2]').config('spark.ui.enabled', 'false')"
                            ".getOrCreate()\n    s.sparkContext.setLogLevel('ERROR')\n    return s")
    env = dict(os.environ, SPARK_UI_PORT=os.environ.get("SPARK_UI_PORT", "4040"))
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=600, env=env)
    print(r.stdout, end="")
    if r.returncode:
        print([l for l in r.stderr.splitlines() if l.strip()][-1][:300])


run(STEP_1)
print()
run(STEP_2)
print()
run(STEP_3, in_memory=True)
