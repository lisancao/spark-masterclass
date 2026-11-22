"""Figures for Chapter 15. Numbers from code/ch15/output/land.txt, shred.txt and speed.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. Key profile of the three days (land.txt)
keys = [("amount", 60, "DECIMAL, OBJECT, STRING", "bad"), ("event", 40, "STRING", "blue"),
        ("event_type", 20, "STRING", "acc"), ("items", 40, "ARRAY", "blue"), ("ts", 60, "STRING", "blue"),
        ("user", 40, "OBJECT (two shapes)", "acc"), ("user_id", 20, "BIGINT", "acc")]
f = Fig(820, 290, "The top-level keys of the three days")
f.text(20, 24, "variant_explode over 60 parsed records: rows with each key, and its kinds of type", 14, 700, anchor="start")
for i, (k, n, t, c) in enumerate(keys):
    y = 42 + i * 32
    f.text(120, y + 18, k, 13, 700, anchor="end", mono=True)
    w = 300 * n / 60
    f.rect(132, y + 5, w, 20, c, r=3)
    f.text(132 + w + 8, y + 20, str(n), 12, 700, anchor="start", c=c)
    f.text(500, y + 20, t, 12, 600, anchor="start", c=c, mono=True)
f.text(20, 280, "Red: one key with three kinds of type. Orange: a renamed or moved field (event_type, user_id and user).", 11,
       400, anchor="start", c="mute", italic=True)
f.save("ch15-key-profile")

# 2. What a query reads: unshredded against shredded (shred.txt)
f = Fig(860, 330, "The Parquet columns of a VARIANT, with and without shredding")
f.text(20, 24, "try_variant_get(v, '$.user.id', 'bigint'): the columns that the scan reads", 14, 700, anchor="start")
f.text(20, 56, "shredding off: 2 binary columns", 13, 700, anchor="start")
f.box(20, 70, 180, 40, "v.metadata", "field names", kind="acc", size=12, mono=True, subsize=10)
f.box(210, 70, 180, 40, "v.value", "all the data", kind="acc", size=12, mono=True, subsize=10)
f.text(20, 132, "reads the full value blob, then looks up user.id", 12, 600, anchor="start", c="acc")
f.text(440, 56, "shredding on: 25 leaf columns, for example", 13, 700, anchor="start")
cols = [("v.metadata", "n"), ("v.value", "n"), ("v.typed_value.amount.value", "n"),
        ("v.typed_value.user.value", "n"), ("v.typed_value.user.typed_value.id.value", "n"),
        ("v.typed_value.user.typed_value.id.typed_value", "ok"), ("v.typed_value.user.typed_value.plan.typed_value", "n"),
        ("v.typed_value.user_id.typed_value", "n")]
for i, (c, k) in enumerate(cols):
    f.box(440, 68 + i * 30, 400, 24, c, kind=k, size=11, mono=True, weight=600 if k == "ok" else 400)
f.text(440, 322, "reads one int64 column (green) for user.id", 12, 600, anchor="start", c="ok")
f.text(20, 190, "amount has only a value column:", 12, 500, anchor="start", c="mute")
f.text(20, 208, "its values are a number, a string or an object,", 12, 500, anchor="start", c="mute")
f.text(20, 226, "so no one type fits enough rows.", 12, 500, anchor="start", c="mute")
f.save("ch15-shredding")

# 3. avg(fare_amount) WHERE payment_type = 1 in seven ways (speed.txt)
q = [("typed columns", 0.33, "9.0 MB", "ok"), ("JSON text, get_json_object", 5.40, "30.1 MB", "bad"),
     ("JSON text, from_json", 5.19, "30.1 MB", "bad"), ("VARIANT unshredded, v:path::type", 1.15, "19.8 MB", "blue"),
     ("VARIANT unshredded, try_variant_get", 1.40, "19.8 MB", "blue"), ("VARIANT shredded, v:path::type", 2.85, "9.0 MB", "acc"),
     ("VARIANT shredded, try_variant_get", 0.31, "9.0 MB", "ok")]
f = Fig(860, 320, "What an extraction costs")
f.text(20, 24, "avg(fare_amount) WHERE payment_type = 1 on 1,000,000 trips: median of 5 runs", 14, 700, anchor="start")
f.text(840, 44, "file size", 11, 700, anchor="end", c="mute")
for i, (lab, s, mb, k) in enumerate(q):
    y = 50 + i * 36
    f.text(300, y + 18, lab, 12.5, 600, anchor="end", mono=True)
    w = 420 * s / 5.40
    f.rect(312, y + 5, w, 22, k, r=3)
    f.text(312 + w + 8, y + 21, f"{s:.2f} s", 12.5, 700, anchor="start", c=k)
    f.text(840, y + 21, mb, 12, 500, anchor="end", c="mute")
f.text(20, 312, "All seven queries give the same answer, 19.769948. One machine, one query shape.", 11, 400, anchor="start",
       c="mute", italic=True)
f.save("ch15-extraction-cost")
