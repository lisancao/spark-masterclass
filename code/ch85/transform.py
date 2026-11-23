"""Chapter 85: script TRANSFORM. Rows go to an external program through stdin and come back through
stdout. No-SerDe mode, ROW FORMAT DELIMITED, Hive SerDe mode, and a session without Hive support.

Run from code/:  uv run python ch85/transform.py > ch85/output/transform.txt
"""
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from hive import work  # noqa: E402

W = work("ch85-transform")
SCRIPT = HERE / "zone_short.py"
BODY = f"""
import re, sys
sys.path.insert(0, {str(HERE)!r})
from hive import runner
spark = make_session()
q = runner(spark)
spark.sql("CREATE TEMPORARY VIEW zones AS SELECT * FROM VALUES "
          "(4, 'Alphabet City'), (24, 'Bloomingdale'), (238, 'Upper West Side North'), (3, 'Allerton/Pelham Gardens') "
          "AS z(id, zone)")
q("ADD FILE '{SCRIPT}'")
def node(sql):
    plan = spark.sql("EXPLAIN " + sql).first()[0]
    return re.findall(r"([A-Za-z]+ScriptTransformation) ", plan)[0]
"""

HIVE_STEPS = """
print("== 1. no ROW FORMAT clause in a Hive session: LazySimpleSerDe, fields split by a tab")
S1 = '''SELECT TRANSFORM (id, zone)
     USING 'python3 zone_short.py'
     AS (id INT, short STRING, letters INT)
     FROM zones'''
q(S1)
print("  physical node:", node(S1))
print("  the IO schema:", re.findall(r"ScriptTransformationIOSchema\\((.{0,95})", spark.sql("EXPLAIN " + S1).first()[0])[0].replace(chr(9), "\\\\t"), "...")

print("== 2. ROW FORMAT DELIMITED sets the separators on each side")
S2 = '''SELECT TRANSFORM (id, zone)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\\\t'
     USING 'python3 zone_short.py'
     AS (id STRING, short STRING, letters STRING)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\\\t'
     FROM zones'''
q(S2)

print("== 3. ROW FORMAT SERDE names the SerDe on each side")
S3 = '''SELECT TRANSFORM (id, zone)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     USING 'python3 zone_short.py'
     AS (id INT, short STRING, letters INT)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     FROM zones'''
q(S3)

print("== 4. no AS clause: two columns, key and value")
q('''SELECT TRANSFORM (id, zone) USING 'python3 zone_short.py' FROM zones''')

print("== 5. a script that fails")
q('''SELECT TRANSFORM (id, zone) USING 'python3 -c "import sys; sys.exit(3)"' AS (a STRING) FROM zones''')
"""

PLAIN_STEPS = """
print("== 6. a session without Hive support: the default field separator is \\\\u0001")
S1 = '''SELECT TRANSFORM (id, zone) USING 'python3 zone_short.py' AS (id INT, short STRING, letters INT) FROM zones'''
print("  physical node:", node(S1))
q(S1)
q('''SELECT TRANSFORM (id, zone)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\\\t'
     USING 'python3 zone_short.py'
     AS (id INT, short STRING, letters INT)
     ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\\\t'
     FROM zones''')
q('''SELECT TRANSFORM (id, zone)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     USING 'python3 zone_short.py' AS (id INT, short STRING, letters INT)
     FROM zones''')
"""

HIVE_SESSION = f"""
from hive import hive_session
def make_session():
    return hive_session("ch85-transform", {str(W)!r})
"""
PLAIN_SESSION = """
from pyspark.sql import SparkSession
def make_session():
    s = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s
"""

for session_code, steps in [(HIVE_SESSION, HIVE_STEPS), (PLAIN_SESSION, PLAIN_STEPS)]:
    code = f"import sys\nsys.path.insert(0, {str(HERE)!r})\n" + session_code + BODY + steps
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=600, cwd=str(W))
    print(r.stdout, end="")
    if r.returncode:
        print([l for l in r.stderr.splitlines() if l.strip()][-1][:300])
