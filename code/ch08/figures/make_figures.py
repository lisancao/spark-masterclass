"""Figures for Chapter 8. Numbers from code/ch08/output/scripting.txt and functions.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. One script with a 5-pass loop against one query (scripting.txt section 4)
f = Fig(820, 250, "What one script with a loop runs")
f.text(20, 26, "One spark.sql() call with a 5-pass WHILE loop", 14, 700, anchor="start")
f.text(20, 52, "20 SQL executions", 13, 600, anchor="start", c="mute")
for i in range(20):
    x = 20 + i * 30
    f.rect(x, 62, 24, 24, "acc" if i < 5 else "n", r=4)
f.text(20, 108, "5 scan the trips", 12, 700, anchor="start", c="acc")
f.text(170, 108, "15 with no scan: conditions, SET, DECLARE, the final SELECT", 12, 500, anchor="start", c="mute")
f.text(640, 52, "27 jobs", 13, 700, anchor="start", c="acc")
f.text(20, 160, "The same total as one query (BETWEEN 132 AND 136)", 14, 700, anchor="start")
f.text(20, 186, "1 SQL execution", 13, 600, anchor="start", c="mute")
f.rect(20, 196, 24, 24, "ok", r=4)
f.text(640, 186, "2 jobs", 13, 700, anchor="start", c="ok")
f.text(60, 214, "total = 149702 in both", 12, 500, anchor="start", c="mute")
f.note("each square is one SQL execution; the order of the colours is schematic")
f.save("ch08-script-executions")

# 2. SQL UDF against Python UDF (functions.txt section 2)
f = Fig(820, 250, "A SQL UDF against a Python UDF")
f.text(20, 26, "The same fare_band logic, WHERE band = 'long' (445,492 rows)", 14, 700, anchor="start")
rows = [("fare_band", "SQL UDF", 0.35, "blue", "Project [CASE WHEN ...]  in generated code"),
        ("fare_band_py", "Python UDF", 1.13, "acc", "ArrowEvalPython  ->  Python worker")]
for i, (name, kind, s, k, plan) in enumerate(rows):
    y = 50 + i * 80
    f.text(20, y + 20, name, 13, 700, anchor="start", mono=True)
    f.text(20, y + 38, kind, 12, 500, anchor="start", c="mute")
    w = 420 * s / 1.13
    f.rect(150, y + 6, w, 28, k, r=4)
    f.text(150 + w + 8, y + 26, f"median {s:.2f} s", 13, 700, anchor="start", c=k)
    f.text(150, y + 54, plan, 12, 500, anchor="start", c=k, mono=True)
f.text(20, 230, "median of three runs after a warmup, one machine", 11, 400, anchor="start", c="mute", italic=True)
f.save("ch08-udf-cost")

# 3. The zone chain of the recursive CTE (functions.txt section 7)
zones = ["Marine Park/Floyd Bennett Field", "Marine Park/Mill Basin", "Flatlands", "Flatbush/Ditmas Park",
         "Park Slope", "Crown Heights North", "Bushwick South"]
f = Fig(820, 300, "The zone chain of the recursive CTE")
f.text(20, 24, "WITH RECURSIVE chain: follow next_zone from zone 154", 14, 700, anchor="start")
pos = [(20, 50), (290, 50), (560, 50), (560, 140), (290, 140), (20, 140), (20, 230)]
for i, (z, (x, y)) in enumerate(zip(zones, pos)):
    f.box(x, y, 240, 48, z, f"step {i}" + ("  (anchor)" if i == 0 else ""), kind="acc" if i == 0 else "blue", size=12, subsize=11)
for (a, b) in zip(pos, pos[1:]):
    (x1, y1), (x2, y2) = a, b
    if y1 == y2:
        f.arrow(x1 + 242 if x2 > x1 else x1 - 2, y1 + 24, x2 - 2 if x2 > x1 else x2 + 242, y2 + 24)
    else:
        f.arrow(x1 + 120, y1 + 50, x2 + 120, y2 - 2)
f.box(290, 230, 300, 48, "next zone is already in path", "WHERE returns no row: the recursion stops", kind="ghost", dash=True, size=12, subsize=11)
f.arrow(262, 254, 288, 254, dash=True)
f.save("ch08-zone-chain")
