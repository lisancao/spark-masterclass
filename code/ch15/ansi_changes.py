"""Chapter 15: what ANSI mode changes, other than the runtime errors of Chapter 10.

Each expression runs with spark.sql.ansi.enabled true (the default since 4.0) and false.
The script prints the value and the type, or the error condition.

Run from code/:  uv run python ch15/ansi_changes.py > ch15/output/ansi_changes.txt
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, work

W = work("ch15-ansi")
spark = session("ch15-ansi", **{"spark.sql.warehouse.dir": str(W / "warehouse")})

EXPRS = [
    "'1' + 1",                      # implicit cast of a string in arithmetic
    "'1.5' + 1",
    "1 = '1.0'",                    # comparison of a number and a string
    "size(NULL)",                   # a NULL array
    "CAST(1.9 AS INT)",             # truncation is not an error
    "7 / 2",
    "CAST('yes' AS BOOLEAN)",
    "CAST('2026-06-01 10:00' AS DATE)",
    "map('a', 1)['b']",
    "element_at(map('a', 1), 'b')",
]


def run(expr):
    try:
        r = spark.sql(f"SELECT {expr} AS v, typeof({expr}) AS t").first()
        return f"{r.v!s} ({r.t})"
    except Exception as e:
        m = re.search(r"\[([A-Z_.]+)\]", str(e))
        return m.group(1) if m else type(e).__name__


print("== 1. expressions, ANSI on and off")
print(f"  {'expression':<36} {'ANSI on':<34} {'ANSI off'}")
for x in EXPRS:
    spark.conf.set("spark.sql.ansi.enabled", "true")
    on = run(x)
    spark.conf.set("spark.sql.ansi.enabled", "false")
    off = run(x)
    print(f"  {x:<36} {on:<34} {off}")
spark.conf.set("spark.sql.ansi.enabled", "true")

print("\n== 2. INSERT follows spark.sql.storeAssignmentPolicy, not ansi.enabled")
print("  spark.sql.storeAssignmentPolicy =", spark.conf.get("spark.sql.storeAssignmentPolicy"))
spark.sql("CREATE TABLE small (n INT) USING parquet")
for ansi in ["true", "false"]:
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    for v in ["3000000000", "'12a'"]:
        print(f"  ansi.enabled={ansi:<5} INSERT {v:<12} -> ", end="")
        try:
            spark.sql(f"INSERT INTO small VALUES ({v})")
            print("accepted")
        except Exception as e:
            print(re.search(r"\[([A-Z_.]+)\]", str(e)).group(1))
spark.conf.set("spark.sql.ansi.enabled", "true")

print("\n== 3. reserved keywords: off by default, also under ANSI")
for flag in ["false", "true"]:
    spark.conf.set("spark.sql.ansi.enforceReservedKeywords", flag)
    try:
        spark.sql("SELECT 1 AS select").collect()
        print(f"  enforceReservedKeywords={flag:<5} SELECT 1 AS select -> accepted")
    except Exception as e:
        print(f"  enforceReservedKeywords={flag:<5} SELECT 1 AS select ->",
              re.search(r"\[([A-Z_.]+)\]", str(e)).group(1))
spark.stop()
