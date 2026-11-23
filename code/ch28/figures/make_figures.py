"""Figures for Chapter 28. Numbers from code/ch28/output/nested.txt and lambdas.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. explode against explode_outer on the tags of 24 orders (nested.txt)
f = Fig(860, 290, "explode against explode_outer")
f.text(20, 24, "The tags of 24 orders: 35 tags, 6 orders with no tags", 14, 700, anchor="start")
for i in range(24):
    x = 20 + (i % 12) * 26
    y = 44 + (i // 12) * 26
    f.rect(x, y, 20, 20, "bad" if i >= 18 else "blue", r=3)
f.text(20, 112, "18 orders with tags", 12, 600, anchor="start", c="blue")
f.text(180, 112, "6 with none", 12, 600, anchor="start", c="bad")
# explode
f.box(360, 40, 230, 54, "explode(tags)", "Filter size(tags) > 0, then Generate", kind="n", size=13, mono=True, subsize=11)
f.box(360, 150, 230, 54, "explode_outer(tags)", "Generate, outer = true, no filter", kind="n", size=13, mono=True, subsize=11)
f.arrow(340, 66, 358, 66); f.arrow(340, 176, 358, 176)
f.path([(330, 66), (340, 66)], head=False); f.path([(330, 66), (330, 176), (340, 176)], head=False)
f.line(326, 66, 330, 66)
def rows(x0, y0, n_ok, n_null):
    for i in range(n_ok + n_null):
        x = x0 + (i % 21) * 11
        y = y0 + (i // 21) * 14
        f.rect(x, y, 8, 10, "ok" if i < n_ok else "bad", r=2, fill=None if i < n_ok else "none")
rows(610, 50, 35, 0)
f.text(610, 102, "35 rows: the 6 orders are gone", 12, 700, anchor="start", c="bad")
rows(610, 160, 35, 6)
f.text(610, 212, "41 rows: 6 with a NULL tag", 12, 700, anchor="start", c="ok")
f.text(20, 270, "The optimizer adds the filter for explode. A row with no elements cannot give an output row.", 11, 400,
       anchor="start", c="mute", italic=True)
f.save("ch28-explode")

# 2. exists and forall with three-valued logic (lambdas.txt)
f = Fig(820, 250, "exists and forall with NULL elements")
cols = ["exists(x > 3)", "forall(x > 0)", "array_contains(5)"]
data = [("array(1, 2)", ["false", "true", "false"]), ("array(1, NULL)", ["NULL", "NULL", "NULL"]),
        ("array(5, NULL)", ["true", "NULL", "true"]), ("array()", ["false", "true", "false"])]
K = {"true": "ok", "false": "bad", "NULL": "n"}
f.text(20, 24, "Three-valued logic: NULL means \"cannot say\"", 14, 700, anchor="start")
for j, c in enumerate(cols):
    f.text(250 + j * 180 + 80, 52, c, 12.5, 700, mono=True)
for i, (a, vals) in enumerate(data):
    y = 64 + i * 38
    f.text(230, y + 22, a, 13, 600, anchor="end", mono=True)
    for j, v in enumerate(vals):
        f.box(250 + j * 180 + 10, y + 4, 140, 28, v, kind=K[v], size=13, mono=True)
f.text(20, 240, "One NULL element can be the match, so exists cannot say false. forall on an empty array is true.", 11, 400,
       anchor="start", c="mute", italic=True)
f.save("ch28-three-valued")
