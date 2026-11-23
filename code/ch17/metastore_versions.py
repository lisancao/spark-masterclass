"""Chapter 17: which Hive metastore versions each Spark release can talk to (from the source at each
release tag), and what 4.3.0 says about a version it dropped or a version that needs other jars.

Run from code/:  uv run python ch17/metastore_versions.py > ch17/output/metastore_versions.txt
Needs ~/spark with the release tags (SPARK_SRC to change it).
"""
import os
import re
import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from hive import work  # noqa: E402

SRC = os.environ.get("SPARK_SRC", str(Path.home() / "spark"))
PKG = "sql/hive/src/main/scala/org/apache/spark/sql/hive/client/package.scala"

print("== 1. HiveVersion objects in sql/hive/.../client/package.scala at each tag")
for tag in ["v3.5.0", "v4.0.0", "v4.1.0", "v4.2.0", "v4.3.0"]:
    text = subprocess.run(["git", "-C", SRC, "show", f"{tag}:{PKG}"], capture_output=True, text=True).stdout
    print(f"  {tag}  {' '.join(re.findall(r'HiveVersion\(\s*\"([0-9.]+)\"', text))}")

print("\n== 2. the Hive jars inside the pyspark 4.3.0 wheel (the 'builtin' client)")
import pyspark  # noqa: E402
jars = sorted(p.name for p in (Path(pyspark.__file__).parent / "jars").glob("hive-*.jar"))
print("  " + "\n  ".join(jars))

print("\n== 3. a session for each setting (one process each: these are static configurations)")
W = work("ch17-versions")
CASES = [("2.3.10", "builtin"), ("1.2.1", "builtin"), ("3.1.3", "builtin")]
for version, jars_conf in CASES:
    code = f"""
import sys, os
sys.path.insert(0, {str(Path(__file__).resolve().parent)!r})
os.environ['SPARK_UI_PORT'] = '0'
from hive import hive_session
folder = {str(W)!r} + '/' + {version!r}
os.makedirs(folder, exist_ok=True)
try:
    s = hive_session('v', folder, **{{'spark.ui.enabled': 'false', 'spark.sql.hive.metastore.version': {version!r},
                                    'spark.sql.hive.metastore.jars': {jars_conf!r}}})
    print('RESULT ok, databases:', [r[0] for r in s.sql('SHOW DATABASES').collect()])
except Exception as e:
    print('RESULT', type(e).__name__ + ':', ' '.join(str(e).split())[:230])
"""
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300)
    line = [l for l in r.stdout.splitlines() if l.startswith("RESULT")]
    print(f"  version={version:<7} jars={jars_conf:<8} {line[0][7:] if line else r.stderr[-300:]}")
