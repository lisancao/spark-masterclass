"""Chapter 4: the modules of Spark 4.3, as the pip package ships them.

Lists the Spark jars in pyspark/jars and the Python packages next to them. No session needed.
Run from code/:  uv run python ch04/modules.py
"""
from pathlib import Path
import pyspark

home = Path(pyspark.__file__).parent
print(f"pyspark {pyspark.__version__}\n\nSpark module jars in pyspark/jars:")
jars = sorted(home.glob("jars/spark-*_2.13-*.jar"))
for j in jars:
    name = j.name.split("_2.13")[0]
    print(f"  {name:<30} {j.stat().st_size / 2**20:6.1f} MiB")
others = len(list(home.glob("jars/*.jar"))) - len(jars)
print(f"  (plus {others} third-party jars)")

print("\nPython packages in pyspark/:")
for d in sorted(p for p in home.iterdir() if p.is_dir() and (p / "__init__.py").exists()):
    print(f"  pyspark.{d.name}")
