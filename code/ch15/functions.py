"""Chapter 15: the built-in function library. Find a function, read its documentation,
and count the functions by group.

The group of each function comes from its ExpressionInfo (the `group` field of the
@ExpressionDescription annotation in the source). Reading it needs the JVM, so section 3
runs on Classic only.

Run from code/:  uv run python ch15/functions.py > ch15/output/functions.txt
"""
import sys
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session

spark = session("ch15-functions")

print("== 1. SHOW FUNCTIONS, and its filters")
for stmt in ["SHOW FUNCTIONS", "SHOW SYSTEM FUNCTIONS", "SHOW USER FUNCTIONS",
             "SHOW FUNCTIONS LIKE 'try_*'", "SHOW FUNCTIONS LIKE 'st_*'"]:
    print(f"  {stmt:<32} {spark.sql(stmt).count():>4} rows")
print("  try_*:", ", ".join(r.function for r in spark.sql("SHOW FUNCTIONS LIKE 'try_*'").collect()))

print("\n== 2. DESCRIBE FUNCTION EXTENDED")
for r in spark.sql("DESCRIBE FUNCTION EXTENDED try_divide").collect():
    print("  " + r.function_desc.replace("\n", "\n  "))

print("\n== 3. the built-in functions by group (from each function's ExpressionInfo)")
jvm = spark.sparkContext._jvm
registry = jvm.org.apache.spark.sql.catalyst.analysis.FunctionRegistry.builtin()
groups = Counter()
since = Counter()
names = registry.listFunction()
it = names.iterator()
while it.hasNext():
    ident = it.next()
    info = registry.lookupFunction(ident).get()
    groups[info.getGroup() or "(no group)"] += 1
    v = info.getSince() or ""
    since[".".join(v.split(".")[:2]) if v.startswith("4.") else "before 4.0"] += 1
print(f"  {'group':<22} functions")
for g, n in groups.most_common():
    print(f"  {g:<22} {n:>4}")
print(f"  {'total':<22} {sum(groups.values()):>4}")
show = {r.function for r in spark.sql("SHOW FUNCTIONS").collect()}
reg = set()
it = names.iterator()
while it.hasNext():
    reg.add(it.next().funcName())
print("  in SHOW FUNCTIONS but not in this registry:", sorted(show - reg))
print("\n  by the 'since' field:", dict(sorted(since.items())))

print("\n== 4. a built-in function has a qualified name too")
spark.sql("SELECT system.builtin.abs(-7) AS a, builtin.abs(-7) AS b, abs(-7) AS c").show()

print("== 5. the same function from Python")
spark.range(1).select(F.try_divide(F.lit(1), F.lit(0)).alias("try_divide")).show()
spark.stop()
