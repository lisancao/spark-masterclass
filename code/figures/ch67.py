"""Figures for Chapter 67. Numbers from code/ch67/output/*.txt."""
from svgkit import *

# 1. Files opened against rows returned (files_pushdown.txt)
q = [("kind = 'image'", 5, 5), ("kind IN ('audio', 'pdf')", 5, 5), ("name LIKE '%.eml'", 4, 4), ("size > 4000", 6, 6),
     ("kind = 'image' AND width > 12", 3, 5), ("kind = 'audio' OR size < 100", 9, 26), ("lower(kind) = 'image'", 5, 26)]
s = Svg(780, 340, "Files that the tasks opened, and rows returned, for each query on the files source")
X0, SC = 260, 17
s.line(X0 + 26 * SC, 30, X0 + 26 * SC, 300, MUTE, 1, dash=True, arrow=False)
s.text(X0 + 26 * SC, 24, "26 files in the folder", 11, MUTE)
for i, (lab, rows, opened) in enumerate(q):
    y = 40 + i * 38
    s.text(X0 - 10, y + 18, lab, 12, MUTE, "end", 600, mono=True)
    full = opened == 26
    s.rect(X0, y + 4, opened * SC, 14, RED if full else BLUE, 1, 2)
    s.text(X0 + opened * SC + 6, y + 16, f"opened {opened}", 11, MUTE, "start", 600)
    s.rect(X0, y + 20, rows * SC, 10, GREEN, 1, 2)
    s.text(X0 + rows * SC + 6, y + 30, f"rows {rows}", 11, MUTE, "start")
ly = 318
for i, (lab, c) in enumerate([("files opened (filter pushed)", BLUE), ("files opened (no useful filter offered)", RED), ("rows returned", GREEN)]):
    s.rect(X0 - 200 + i * 230, ly - 9, 14, 10, c, 1, 2)
    s.text(X0 - 180 + i * 230, ly, lab, 11, MUTE, "start")
s.save(FIG + "ch67-files-opened.svg")

# 2. Spark alone, v2 params and v3 guarded (sqlite_pushdown.txt)
rows = [("amount > 100", 680, 759, 680), ("qty > 3", 601, 610, 601), ("status LIKE 'ship%'", 329, 650, 329),
        ("amount IS NULL", 104, 25, 104), ("qty IN (2, 4)", 376, 376, 376), ("customer = 'ana'", 121, 121, 121),
        ("placed_at >= '2026-09-15'", 620, 404, 620)]
s = Svg(780, 380, "Row counts of seven filters: Spark alone, pushed with parameters, pushed with guards")
X0, SC = 230, 0.62
for i, (lab, sp, v2, v3) in enumerate(rows):
    y = 30 + i * 46
    s.text(X0 - 10, y + 22, lab, 12, MUTE, "end", 600, mono=True)
    for k, (v, c) in enumerate([(sp, GRAY), (v2, RED if v2 != sp else BLUE), (v3, GREEN)]):
        s.rect(X0, y + 2 + k * 13, v * SC, 11, c, 1, 2)
        s.text(X0 + v * SC + 5, y + 12 + k * 13, str(v) + ("  wrong" if k == 1 and v2 != sp else ""), 10,
               ACC if k == 1 and v2 != sp else MUTE, "start", 700 if k == 1 and v2 != sp else 400)
ly = 360
for i, (lab, c) in enumerate([("Spark alone", GRAY), ("v2: parameters (wrong: red)", RED), ("v3: parameters and type guards", GREEN)]):
    s.rect(X0 - 150 + i * 210, ly - 9, 14, 10, c, 1, 2)
    s.text(X0 - 130 + i * 210, ly, lab, 11, MUTE, "start")
s.save(FIG + "ch67-guarded.svg")
