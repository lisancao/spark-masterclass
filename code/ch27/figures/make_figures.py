"""Figures for Chapter 27. Numbers from code/ch27/output/infer_cost.txt, infer.txt and drift.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. The cost of inference on one million trips as JSON Lines (infer_cost.txt)
f = Fig(820, 270, "The cost of JSON inference")
f.text(20, 24, "1,000,000 trips as JSON Lines (450.6 MB): median of 5 runs", 14, 700, anchor="start")
rows = [("inference only", [("no schema", 0.60, "acc"), ("samplingRatio=0.1", 0.19, "purple")]),
        ("count()", [("no schema", 0.94, "acc"), ("explicit schema", 0.39, "ok")]),
        ("sum(fare_amount)", [("no schema", 1.19, "acc"), ("explicit schema", 0.42, "ok")])]
y = 44
for lab, bars in rows:
    f.text(150, y + 30, lab, 13, 700, anchor="end")
    for i, (b, s, k) in enumerate(bars):
        yy = y + i * 26
        w = 440 * s / 1.19
        f.rect(162, yy + 6, w, 20, k, r=3)
        f.text(162 + w + 8, yy + 21, f"{s:.2f} s  {b}", 12, 600, anchor="start", c=k)
    y += 70
f.save("ch27-infer-cost")

# 2. How inference merges two types of one field (infer.txt)
f = Fig(860, 300, "How JSON inference merges the types of a field")
f.text(20, 24, "Two values of one field, and the type that inference gives (from infer.py)", 14, 700, anchor="start")
pairs = [("1 + 2", "bigint", "ok", "integers are bigint, never int"),
         ("1 + 2.5", "double", "ok", "two numbers: the wider type"),
         ("1 + 99999999999999999999", "decimal(20,0)", "ok", "too large for bigint"),
         ("1 + \"2\"", "string", "bad", "different kinds: the text stays, the type goes"),
         ("1 + true", "string", "bad", ""),
         ("1 + {\"x\": 1}", "string", "bad", ""),
         ("{\"x\": 1} + {\"y\": \"a\"}", "struct<x,y>", "blue", "two objects merge field by field"),
         ("null + null", "string", "n", "null has no type"),
         ("\"2026-10-01\"", "string", "n", "no date inference by default")]
for i, (a, t, k, why) in enumerate(pairs):
    y = 40 + i * 28
    f.text(250, y + 17, a, 12.5, 500, anchor="end", mono=True)
    f.arrow(260, y + 13, 300, y + 13)
    f.box(306, y + 2, 150, 22, t, kind=k, size=12, mono=True, weight=600)
    f.text(470, y + 17, why, 12, 500, anchor="start", c="mute")
f.save("ch27-type-merge")

# 3. The books balance, file by file (drift.txt)
data = [("events_2026-10-01.json", 20, 20, 0), ("events_2026-10-02.json", 20, 20, 0), ("events_2026-10-03.json", 21, 20, 1)]
f = Fig(820, 200, "Each record is in silver or in the quarantine")
f.text(20, 24, "read = silver + quarantine, file by file", 14, 700, anchor="start")
for i, (fn, r, s, q) in enumerate(data):
    y = 44 + i * 40
    f.text(220, y + 18, fn, 12.5, 600, anchor="end", mono=True)
    for j in range(r):
        f.rect(232 + j * 22, y + 4, 18, 22, "ok" if j < s else "bad", r=3)
    f.text(232 + 21 * 22 + 10, y + 20, f"{r} = {s} + {q}", 13, 700, anchor="start", c="bad" if q else "ok")
f.rect(232, 172, 14, 14, "ok", r=2); f.text(252, 184, "silver row", 11, 500, anchor="start", c="mute")
f.rect(342, 172, 14, 14, "bad", r=2); f.text(362, 184, "quarantine: the truncated last line of day 3", 11, 500, anchor="start", c="mute")
f.save("ch27-balance")
