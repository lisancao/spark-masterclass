"""Chapter 85: what enableHiveSupport() changes, and what the default Derby metastore puts on disk.

Run from code/:  uv run python ch85/hive_session.py > ch85/output/hive_session.txt
"""
import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from hive import hive_session, runner, work, files  # noqa: E402

W = work("ch85-session")
print("== 1. the folder before the session starts")
files(W, "contents")

spark = hive_session("ch85-session", W)
q = runner(spark)

print("\n== 2. a table, and the folder after the first statement")
q("CREATE DATABASE taxi")
q("CREATE TABLE taxi.zones (id INT, borough STRING) STORED AS PARQUET")
q("INSERT INTO taxi.zones VALUES (1, 'EWR'), (2, 'Queens')")
print("  top level:", sorted(p.name for p in W.iterdir()))
files(W / "warehouse", "warehouse")
n = sum(1 for _ in (W / "metastore_db").rglob("*") if _.is_file())
print(f"  metastore_db: a Derby database, {n} files, for example "
      f"{sorted(p.name for p in (W / 'metastore_db').iterdir())[:5]}")

print("\n== 3. the catalog, and the Hive client that Spark uses")
for k in ["spark.sql.catalogImplementation", "spark.sql.hive.metastore.version",
          "spark.sql.hive.metastore.jars", "spark.sql.hive.convertMetastoreParquet",
          "spark.sql.hive.convertMetastoreOrc", "spark.sql.legacy.createHiveTableByDefault"]:
    print(f"  {k:<44} {spark.sql(f'SET {k}').first()[1]}")
print("  external catalog class:",
      spark._jsparkSession.sharedState().externalCatalog().unwrapped().getClass().getSimpleName())

print("\n== 4. a second process on the same Derby folder, while this one runs")
code = (f"import sys; sys.path.insert(0, {str(Path(__file__).resolve().parent)!r});"
        "from hive import hive_session; import os;"
        f"os.environ['SPARK_UI_PORT'] = '0';"
        f"s = hive_session('second', {str(W)!r}, **{{'spark.ui.enabled': 'false'}});"
        "print('  second process sees:', [r[0] for r in s.sql('SHOW DATABASES').collect()])")
r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300)
err = [l for l in (r.stdout + r.stderr).splitlines() if "Another instance of Derby" in l or "second process" in l]
print("  " + (err[0].split("Caused by: ")[-1].replace(str(W), "<folder>").strip() if err else "(no output)"))
print("  exit code:", r.returncode)

