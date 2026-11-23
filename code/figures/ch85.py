"""Figures for Chapter 85. Numbers and plan nodes from code/ch85/output/*.txt."""
import re
from svgkit import *

# 1. bench.txt as bars, coloured by the Python node of the plan
rows, part = [], None
for ln in open("../ch85/output/bench.txt"):
    if ln.startswith("A.") or ln.startswith("B."):
        part = ln.strip(); rows.append(("H", part)); continue
    m = re.match(r"\s{2}(.+?)\s{2,}([\d.]+) s\s+[\d.]+x\s+(no Python|ArrowEvalPython|BatchEvalPython)", ln)
    if m:
        rows.append((m[1], float(m[2]), m[3]))
s = Svg(780, 30 + 26 * len(rows) + 40, "Median seconds of each version of is_png and fare_band")
X0, SC = 330, 120
y = 20
for r in rows:
    if r[0] == "H":
        s.text(10, y + 16, r[1], 13, ACC, "start", 700); y += 26; continue
    lab, v, node = r
    c = GREEN if node == "no Python" else (ORANGE if node == "ArrowEvalPython" else RED)
    s.text(X0 - 8, y + 15, lab, 11, MUTE, "end", 600 if "PngRule" in lab or "transpiled" in lab else 400)
    s.rect(X0, y + 4, v * SC, 15, c, 1, 2)
    s.text(X0 + v * SC + 6, y + 16, f"{v:.3f} s", 11, MUTE, "start", 700)
    y += 26
y += 10
for i, (lab, c) in enumerate([("no Python node", GREEN), ("ArrowEvalPython", ORANGE), ("BatchEvalPython", RED)]):
    s.rect(X0 + i * 150, y - 9, 14, 10, c, 1, 2)
    s.text(X0 + 20 + i * 150, y, lab, 11, MUTE, "start")
s.text(10, y + 22, "Median of 5 runs after a warm-up, a new DataFrame for each run, local[*] on 16 cores.", 11, MUTE, "start", italic=True)
s.h = y + 32
s.save(FIG + "ch85-bench.svg")

# 2. The plan before and after PngRule (extensions.txt)
def tree(s, x, y, nodes, w):
    for i, (lab, c) in enumerate(nodes):
        s.box(x, y + i * 48, w, 34, lab, c, 11, mono=True)
        if i:
            s.line(x + w / 2, y + i * 48 - 1, x + w / 2, y + i * 48 - 13, MUTE, 1.5, arrow=False)
s = Svg(780, 250, "Physical plans of filter(udf(content)) for a UDF that the rule ignores and for one that it rewrites")
s.text(190, 22, "looks_like_png: not rewritten", 13, MUTE, "middle", 700)
s.text(590, 22, "is_png: rewritten by jext.PngRule", 13, ACC, "middle", 700)
tree(s, 20, 40, [("Project [path]", GRAY), ("Filter pythonUDF0", GRAY), ("ArrowEvalPython [looks_like_png(content)]", ORANGE), ("FileScan binaryFile", BLUE)], 340)
tree(s, 420, 40, [("Project [path]", GRAY), ("Filter coalesce(is_png(content), false)", GREEN), ("FileScan binaryFile", BLUE)], 340)
s.text(190, 240, "the bytes of each file go to a Python worker", 11, MUTE, "middle", italic=True)
s.text(590, 200, "no Python node: the native expression runs in the JVM", 11, MUTE, "middle", italic=True)
s.text(590, 218, "coalesce keeps the False of the UDF for a NULL input", 11, MUTE, "middle", italic=True)
s.save(FIG + "ch85-rule-plans.svg")
