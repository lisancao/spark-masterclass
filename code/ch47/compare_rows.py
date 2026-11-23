"""Chapter 47: print the rows of one query, so two engines' answers can be compared by eye.

Run with the Python of the engine's Spark version, for example:
  ~/spark-masterclass/data/ch47/venv-4.2.0/bin/python ch47/compare_rows.py rapids boroughs
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ch47.engines import start, queries, OFFLOAD_FRIENDLY

engine, name = sys.argv[1], sys.argv[2]
spark = start(engine, **OFFLOAD_FRIENDLY)
print(f"{engine} on Spark {spark.version}, {name}:")
for row in queries(spark)[name].collect():
    print("  " + "  ".join(f"{v!s:>14}" for v in row))
