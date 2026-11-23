"""Figures for Chapter 83. Numbers from code/ch83/output/groupby.txt, sets.txt and bench.txt."""
from pathlib import Path
from svgkit import *

OUT = str(Path(__file__).resolve().parents[2] / "book" / "media" / "figures") + "/"

# 1. Rows through the two aggregate phases (groupby.txt sections 1 and 4)
s = Svg(820, 400, "Rows out of each node: a key that repeats and a key that hardly repeats")
s.text(20, 26, "rows out of each node, June 2026 trips (log scale bars)", 14, MUTE, "start", 600)
import math
lanes = [
    ("GROUP BY payment_type", [("scan", 3837248), ("filter + join", 3663191), ("partial agg", 14), ("final agg", 5)], BLUE),
    ("GROUP BY tpep_pickup_datetime", [("scan", 3837248), ("filter + join", 3663191), ("partial agg", 2228358), ("final agg", 1763643)], ORANGE),
]
X0, MAXW = 190, 470
y = 52
for title, steps, col in lanes:
    s.text(20, y + 8, title, 15, ACC if col == ORANGE else MUTE, "start", 700)
    y += 22
    for name, n in steps:
        w = MAXW * math.log10(max(n, 1) + 1) / math.log10(3837248 + 1)
        s.text(X0 - 12, y + 16, name, 13, MUTE, "end")
        s.rect(X0, y + 2, max(w, 3), 20, col, 0.9 if name != "partial agg" else 1.0, 3)
        s.text(X0 + max(w, 3) + 8, y + 17, f"{n:,}", 13, MUTE, "start", 600 if name == "partial agg" else 400)
        y += 28
    y += 18
s.text(410, y + 4, "Few keys: the partial step sends 14 rows to the shuffle. Unique keys: it removes only 39%.", 13, MUTE)
s.save(OUT + "ch83-two-phase.svg")

# 2. Which operator: the buffer type decides (groupby.txt section 2, AggUtils.createAggregate)
s = Svg(820, 360, "Which aggregate operator Spark 4.3.0 plans")
s.box(20, 20, 250, 64, "Every aggregation buffer\nis a fixed-width value?", GRAY, 14)
s.box(20, 140, 250, 64, "Some aggregate is a\nTypedImperativeAggregate?", GRAY, 14)
s.box(330, 20, 210, 64, "HashAggregate", BLUE, 16, sub="sum, count, avg, max(number),\napprox_count_distinct", subsize=11)
s.box(330, 140, 210, 64, "ObjectHashAggregate", GREEN, 16, sub="percentile_approx, collect_list,\nsketches", subsize=11)
s.box(330, 260, 210, 64, "SortAggregate", ORANGE, 16, sub="max(string), first(string)", subsize=11)
s.line(270, 52, 328, 52); s.text(299, 44, "yes", 12, MUTE)
s.line(145, 84, 145, 138); s.text(160, 116, "no", 12, MUTE, "start")
s.line(270, 172, 328, 172); s.text(299, 164, "yes", 12, MUTE)
s.path("M145,204 L145,292 L328,292"); s.text(160, 250, "no", 12, MUTE, "start")
s.frame(570, 20, 235, 304, "and also", MUTE, 13)
notes = ["Grouping keys must be", "binary stable, or Spark", "uses SortAggregate.", "",
         "ObjectHashAggregate sorts", "after 128 keys in a task:", "4 tasks fell back for", "GROUP BY PULocationID.", "",
         "4.3: a sorted child turns", "a hash aggregate into a", "SortAggregate."]
for i, t in enumerate(notes):
    s.text(586, 62 + i * 20, t, 13, MUTE, "start")
s.save(OUT + "ch83-operators.svg")

# 3. grouping_id bits for CUBE(Borough, pay) (sets.txt section 3)
s = Svg(820, 300, "grouping_id() for CUBE(Borough, pay)")
heads = ["grouping_id()", "bits", "Borough", "pay", "rows (trips)"]
xs = [40, 170, 270, 420, 570]
for x, h in zip(xs, heads):
    s.text(x, 34, h, 14, MUTE, "start", 700)
rows = [(0, "00", "Borough", "pay", "6 rows, e.g. Queens card 207,275"),
        (1, "01", "Borough", "(all)", "3 rows, e.g. Queens 260,235"),
        (2, "10", "(all)", "pay", "2 rows, card 2,400,059"),
        (3, "11", "(all)", "(all)", "1 row, 2,738,727")]
for j, (g, b, bo, p, r) in enumerate(rows):
    y = 56 + j * 54
    s.box(30, y, 110, 40, str(g), BLUE, 16)
    s.box(160, y, 80, 40, b, ORANGE, 16, mono=True)
    s.box(260, y, 130, 40, bo, GRAY if bo == "(all)" else GREEN, 14)
    s.box(410, y, 130, 40, p, GRAY if p == "(all)" else GREEN, 14)
    s.text(560, y + 25, r, 13, MUTE, "start")
s.text(410, 286, "A bit is 1 when its column is not in the grouping set. The first column is the high bit.", 13, ACC)
s.save(OUT + "ch83-grouping-id.svg")
print("ok")
