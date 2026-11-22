"""Chapter 24: what an engine did to each taxi query's plan.

Prints each query's executed plan as a tree of operator classes, marked by where it ran:
  E = in the native or GPU engine,  S = in Spark's own JVM code,  T = a row/columnar transition,
  . = glue (stage wrappers, shuffle readers, batch resizers).
Run with the Python of the engine's Spark version, for example:
  ~/spark-masterclass/data/ch24/venv-4.2.0/bin/python ch24/plans.py comet
  .venv/bin/python ch24/plans.py cpu                  # Spark 4.3.0, no engine
Add --defaults to keep Spark's default settings (ANSI on, TIMESTAMP_NTZ inference on).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ch24.engines import start, queries, udf_query, walk, kind, native_share, OFFLOAD_FRIENDLY

engine = sys.argv[1]
defaults = "--defaults" in sys.argv
only = [a for a in sys.argv[2:] if not a.startswith("--")]
extra = {} if defaults else dict(OFFLOAD_FRIENDLY)
for a in sys.argv[2:]:
    if a.startswith("--conf="):
        k, v = a[len("--conf="):].split("=", 1)
        extra[k] = v
spark = start(engine, **extra)
MARK = {"engine": "E", "spark": "S", "transition": "T", "glue": "."}

print(f"{engine} on Spark {spark.version}, "
      f"{'default settings' if defaults else 'offload-friendly settings'}")
todo = queries(spark)
todo["udf"] = udf_query(spark)
for name, df in todo.items():
    if only and name not in only:
        continue
    df.collect()
    nodes = walk(df)
    e, s, t = native_share(nodes)
    print(f"\n{name}: {e} engine operators, {s} Spark operators, {t} transitions")
    for depth, op, _ in nodes:
        print(f"  {MARK[kind(op)]} {'  ' * depth}{op}")
