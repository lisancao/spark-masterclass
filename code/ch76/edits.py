"""Chapter 76, New in 4.3: variant_set, try_variant_set, variant_delete and variant_strip_nulls, at the byte level.
Each edit returns a new value and a new metadata blob; the script shows what changed.

Run from code/:  uv run python ch76/edits.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.book import session
from variant_codec import parse_metadata, object_layout, spaced, type_name

spark = session("ch76-edits")

def show(label, v):
    val, meta = bytes(v.value), bytes(v.metadata)
    md = parse_metadata(meta)
    L = object_layout(val, 0) if type_name(val) == "OBJECT" else None
    widths = f"id size {L['id_size']}, offset size {L['offset_size']}" if L else ""
    j = v.toJson()
    print(f"  {label:<30} {j if len(j) <= 60 else j[:57] + '...'}")
    print(f"  {'':<30} keys {md['keys']}; value {len(val)} B ({widths}); metadata {len(meta)} B")

base = """parse_json('{"event": "view", "user_id": 7, "note": null}')"""
print("--- one event, then each 4.3 edit function on it")
for label, e in [("original", base),
                 ("variant_set $.user_id = 8", f"variant_set({base}, '$.user_id', 8)"),
                 ("variant_set $.plan (new key)", f"variant_set({base}, '$.plan', 'pro')"),
                 ("variant_delete $.event", f"variant_delete({base}, '$.event')"),
                 ("variant_strip_nulls", f"variant_strip_nulls({base})"),
                 ("set $.plan, create_if_missing", f"variant_set({base}, '$.plan', 'pro', false)")]:
    show(label, spark.sql(f"SELECT {e} AS v").first().v)

print("\n--- an edit can change the widths of the whole object")
big = "parse_json('{\"a\": \"" + "x" * 60 + "\", \"b\": \"" + "y" * 60 + "\", \"c\": \"" + "z" * 60 + "\"}')"
show("three strings of 60 bytes", spark.sql(f"SELECT {big} AS v").first().v)
show("variant_set $.d = 80 bytes", spark.sql(f"SELECT variant_set({big}, '$.d', '{'w' * 80}') AS v").first().v)

print("\n--- the edited bytes: set $.user_id = 8")
before = spark.sql(f"SELECT {base} AS v").first().v
after = spark.sql(f"SELECT variant_set({base}, '$.user_id', 8) AS v").first().v
print("  before", spaced(bytes(before.value)))
print("  after ", spaced(bytes(after.value)))

print("\n--- errors and try_")
for e in [f"variant_set({base}, '$.event.name', 'x')", f"try_variant_set({base}, '$.event.name', 'x')",
          f"variant_set({base}, '$', 1)"]:
    try:
        print(f"  {e[:60]:<62} -> {spark.sql(f'SELECT to_json({e})').first()[0]}")
    except Exception as ex:
        import re
        m = re.search(r"\[[A-Z_.]+\][^\n]*", str(ex))
        print(f"  {e[:60]:<62} -> {(m.group(0) if m else str(ex))[:70]}")
