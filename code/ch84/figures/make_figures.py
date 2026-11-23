"""Figures for Chapter 84. Values from code/ch84/output/partition_overwrite.txt and view_schema.txt.
Run: FIG_CHECK_DIR=<scratch> python3 code/ch84/figures/make_figures.py"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. Static against dynamic partition overwrite (partition_overwrite.txt, sections 1, 2, 4, 5).
DAYS = ["2026-06-01", "2026-06-02", "2026-06-03"]
RUNS = [
    ("STATIC, rerun of June 2 (section 1)", ["deleted", "replaced", "deleted"]),
    ("DYNAMIC, rerun of June 2 (section 2)", ["kept", "replaced", "kept"]),
    ("STATIC, empty result (section 5)", ["deleted", "deleted", "deleted"]),
    ("DYNAMIC, empty result (section 4)", ["kept", "kept", "kept"]),
]
KIND = {"deleted": "bad", "replaced": "acc", "kept": "ok"}
f = Fig(940, 420, "Static and dynamic partition overwrite")
f.text(20, 30, "INSERT OVERWRITE TABLE by_day SELECT ...   (no PARTITION clause)", 15, 700, anchor="start", mono=True)
f.text(20, 54, "before each run: day=2026-06-01, day=2026-06-02, day=2026-06-03, one file each", 12.5, 400,
       anchor="start", c="mute")
for i, (label, states) in enumerate(RUNS):
    col, row = i % 2, i // 2
    x, y = 20 + col * 460, 80 + row * 165
    f.box(x, y, 440, 150, kind="ghost")
    f.text(x + 14, y + 24, label, 14, 700, anchor="start")
    for j, (d, st) in enumerate(zip(DAYS, states)):
        bx = x + 14 + j * 140
        f.box(bx, y + 44, 130, 52, f"day={d[5:]}", st, kind=KIND[st], mono=True, size=13)
    left = sum(s != "deleted" for s in states)
    f.text(x + 14, y + 128, f"partitions after: {left} of 3", 12.5, 600, anchor="start",
           c="bad" if left < 3 else "ok")
f.save("ch84-partition-overwrite")

# 2. View schema modes after table changes (view_schema.txt, section 2).
cols = ["mode", "INT -> BIGINT", "INT -> STRING ('x2')", "column added", "column dropped"]
grid = [
    ["BINDING", "CANNOT_UP_CAST", "CANNOT_UP_CAST", "ok, 2 columns", "INCOMPATIBLE"],
    ["COMPENSATION (default)", "ok, id int", "CAST_INVALID_INPUT", "ok, 2 columns", "INCOMPATIBLE"],
    ["TYPE EVOLUTION", "ok, id bigint", "ok, id string", "ok, 2 columns", "INCOMPATIBLE"],
    ["EVOLUTION", "ok, id bigint", "ok, id string", "ok, 3 columns", "ok, 1 column"],
]
f = Fig(940, 330, "View schema modes after table changes")
f.text(20, 30, "CREATE VIEW v WITH SCHEMA <mode> AS SELECT * FROM zones   (zones: id INT, borough STRING)",
       14, 700, anchor="start", mono=True)
X0, Y0, W0, WC, RH = 20, 56, 200, 175, 50
for j, c in enumerate(cols):
    x = X0 if j == 0 else X0 + W0 + (j - 1) * WC
    w = W0 if j == 0 else WC
    f.box(x, Y0, w - 6, 34, c, kind="n", size=12.5, mono=(j > 0))
for i, row in enumerate(grid):
    y = Y0 + 44 + i * RH
    for j, cell in enumerate(row):
        x = X0 if j == 0 else X0 + W0 + (j - 1) * WC
        w = W0 if j == 0 else WC
        if j == 0:
            f.box(x, y, w - 6, RH - 8, cell, kind="blue", size=12.5, mono=True)
        else:
            ok = cell.startswith("ok")
            f.box(x, y, w - 6, RH - 8, cell, kind="ok" if ok else "bad", size=12, mono=not ok)
f.text(20, 318, "red: the query on the view fails with this error class. green: the view returns rows.",
       11.5, 400, anchor="start", c="mute", italic=True)
f.save("ch84-view-modes")
