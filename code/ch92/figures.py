"""Figures for Chapter 92. Plans and times from code/ch92/output/datasets.txt.

Run from code/:  uv run python ch92/figures.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "code" / "common"))
from svgfig import Fig

# 1. the physical plans of sections 3 and 4, node by node
f = Fig(800, 360, "Typed and untyped plans for the same logic")
f.text(20, 28, "trips.filter(t => t.fare_amount > 0).map(...)  against  where(...).select(...)", 14, 700, anchor="start", mono=True)
typed = [("SerializeFromObject", "Tuple2 -> row", "acc"), ("MapElements", "lambda on a Trip object", "acc"),
         ("Filter", "invoke(lambda.apply(obj))", "acc"), ("DeserializeToObject", "row -> new Trip(...)", "acc"),
         ("ColumnarToRow", "", "n"), ("FileScan parquet", "6 columns, DataFilters: []", "n")]
untyped = [("Project", "PULocationID, tip_amount / fare_amount", "blue"),
           ("Filter", "isnotnull(fare_amount) AND fare_amount > 0.0", "blue"),
           ("ColumnarToRow", "", "n"), ("FileScan parquet", "3 columns", "n")]
for col, (x, title, nodes) in enumerate(((30, "typed (Dataset[Trip])", typed), (430, "untyped (DataFrame)", untyped))):
    f.text(x, 62, title, 13, 700, anchor="start", c="acc" if col == 0 else "blue")
    for i, (n, sub, k) in enumerate(nodes):
        y = 74 + i * 46
        f.box(x, y, 340, 38, n, sub, kind=k, size=12.5, subsize=10.5, mono=True)
        if i:
            f.line(x + 170, y - 8, x + 170, y, c="line")
f.text(430, 290, "the lambda is a black box: no pushdown,", 12, 600, anchor="start", c="mute")
f.text(430, 308, "and each row becomes a JVM object and back", 12, 600, anchor="start", c="mute")
f.text(780, 350, "physical plans from datasets.txt, Spark 4.3.0 Classic", 11, 400, anchor="end", c="mute", italic=True)
f.save("ch92-plans")

# 2. times (section 5 and the end of section 7), median of 5 after a warmup, under the bench lock
rows = [("typed filter + map", 0.21, "acc"), ("select first, then typed", 0.16, "acc"), ("untyped where + select", 0.11, "blue"),
        ("groupByKey + TipShare Aggregator", 0.63, "acc"), ("groupBy + avg(when(...))", 0.12, "blue"),
        ("mean_of: Aggregator through udaf()", 0.39, "acc"), ("avg (built-in)", 0.10, "blue")]
f = Fig(800, 360, "Typed against untyped, measured")
f.text(20, 28, "Seconds over 3,837,248 trips: typed code in orange, untyped in blue", 15, 700, anchor="start")
X0 = 300
for i, (n, v, k) in enumerate(rows):
    y = 52 + i * 36 + (14 if i >= 3 else 0) + (14 if i >= 5 else 0)
    f.text(X0 - 12, y + 18, n, 12.5, 600, anchor="end", mono=True)
    w = 420 * v / 0.7
    f.rect(X0, y + 4, w, 22, k, r=3)
    f.text(X0 + w + 8, y + 20, f"{v:.2f} s", 12, 700, anchor="start", c=k)
f.text(20, 350, "median of 5 runs after a warmup, local[*], 16 cores, under the bench lock", 11, 400, anchor="start",
       c="mute", italic=True)
f.save("ch92-times")
