"""Chapter 84: what the META-INF/services file does. The same jar, without it.

Run from code/:  uv run python ch84/no_service.py
"""
import shutil, subprocess, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import JAR, docs_root  # noqa: E402
from common.book import session  # noqa: E402

bare = Path(tempfile.gettempdir()) / "jdocs-no-service.jar"
shutil.copy(JAR, bare)
subprocess.run(["zip", "-q", "-d", str(bare), "META-INF/services/*"], check=True)
print("jar entries under META-INF/services:",
      subprocess.run(["unzip", "-l", str(bare), "META-INF/services/*"],
                     capture_output=True, text=True).stdout.count("services/"))

spark = session("ch84-no-service", spark_jars=str(bare))
root = docs_root()
for name in ("jdocs", "jdocs.DocsSource"):
    try:
        n = spark.read.format(name).option("path", root).load().count()
        print(f"format({name!r}): {n} rows")
    except Exception as e:
        lines = str(e).splitlines()
        print(f"format({name!r}): {type(e).__name__}")
        for key in ("DATA_SOURCE_NOT_FOUND", "Caused by: java.lang.ClassNotFoundException"):
            line = next(l.strip() for l in lines if key in l).lstrip(": ")
            print("  " + line[:150])
spark.stop()
