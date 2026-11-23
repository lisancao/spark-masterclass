"""Chapter 90, Step 5: how parse_json picks a primitive type for each JSON number, what typed casts to
VARIANT write, and what schema_of_variant and to_json hide.

Run from code/:  uv run python ch90/encodings.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.book import session
from variant_codec import encode_json, type_name, spaced, decode

spark = session("ch90-encodings")

inputs = ["1", "-128", "128", "40000", "3000000000", "9223372036854775807", "9223372036854775808",
          "123456789012345678901234567890123456789", "1.0", "1.50", "6.13", "0.000000001",
          "12345678901.5", "1e2", "1.5E-3", '"x"', "true", "null"]
print(f"{'JSON text':<42} {'type':<10} {'bytes':<26} {'schema_of_variant':<17} matches Spark")
for t in inputs:
    r = spark.sql(f"SELECT parse_json('{t}') AS v, schema_of_variant(parse_json('{t}')) AS s").first()
    v = bytes(r.v.value)
    mine, _ = encode_json(t)
    shown = spaced(v) if len(v) <= 8 else spaced(v[:8]) + " ..."
    print(f"{t:<42} {type_name(v):<10} {shown:<26} {r.s:<17} {mine == v}")

print("\nTwo rows of one field: what schema_of_variant_agg says")
for a, b in [("1", "2.5"), ("1", "2.5e0"), ("1", "3000000000"), ("1.5", "1.25")]:
    s = spark.sql(f"""SELECT schema_of_variant_agg(v) AS s FROM
                      (SELECT parse_json('{{"x": {a}}}') v UNION ALL SELECT parse_json('{{"x": {b}}}'))""").first().s
    print(f"  x = {a:<4} and x = {b:<11} -> {s}")

print("\nTyped values cast to VARIANT (not from JSON text)")
exprs = [("DATE'2026-10-01'", "date"), ("TIMESTAMP'2026-10-01 10:00:00'", "timestamp"),
         ("TIMESTAMP_NTZ'2026-10-01 10:00:00'", "timestamp_ntz"), ("CAST(1.5 AS FLOAT)", "float"),
         ("CAST(1.5 AS DOUBLE)", "double"), ("X'CAFE'", "binary"), ("CAST(7 AS SMALLINT)", "smallint"),
         ("CAST(7 AS BIGINT)", "bigint"), ("CAST(1.50 AS DECIMAL(10,2))", "decimal(10,2)")]
for e, label in exprs:
    r = spark.sql(f"SELECT CAST({e} AS VARIANT) AS v, to_json(CAST({e} AS VARIANT)) AS j").first()
    v = bytes(r.v.value)
    print(f"  {label:<14} {type_name(v):<14} {spaced(v):<30} to_json {r.j:<24} back: {decode(v, [])!r}")

print("\nto_json loses the type: the text goes back through parse_json as a different type")
for e in ["DATE'2026-10-01'", "X'CAFE'", "CAST(1.5 AS DOUBLE)", "CAST(7 AS BIGINT)"]:
    r = spark.sql(f"""SELECT CAST({e} AS VARIANT) AS v,
                      parse_json(to_json(CAST({e} AS VARIANT))) AS w""").first()
    print(f"  {e:<22} {type_name(bytes(r.v.value)):<9} -> to_json -> parse_json -> {type_name(bytes(r.w.value))}")
