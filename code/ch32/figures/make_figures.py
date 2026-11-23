"""Figures for Chapter 32. Lines from code/data/messy/logs/app.log; numbers from code/ch32/output/logs.txt and csv_drift.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

ROOT = Path(__file__).resolve().parents[2]
lines = (ROOT / "data/messy/logs/app.log").read_text().splitlines()[:8]

# 1. Split the whole file where a new event starts (lookahead)
f = Fig(880, 330, "Split a log where a new event starts")
f.text(20, 24, "split(value, \"\\n(?=\\d{4}-\\d{2}-\\d{2} |\\{)\"): the first 8 lines of app.log", 13, 700, anchor="start", mono=True)
import re
ev = 0
y = 48
starts = [i for i, l in enumerate(lines) if re.match(r"\d{4}-\d{2}-\d{2} |\{", l)]
groups = []
for i, l in enumerate(lines):
    if i in starts:
        groups.append([i])
    else:
        groups[-1].append(i)
LH = 30
for g, idx in enumerate(groups):
    y0 = 48 + idx[0] * LH
    h = len(idx) * LH
    k = "bad" if len(idx) > 1 else "blue"
    f.rect(90, y0 - 2, 770, h - 4, k, r=6)
    f.text(80, y0 + h / 2 + 2, f"event {g + 1}", 12, 700, anchor="end", c=k)
    for j, i in enumerate(idx):
        txt = lines[i].replace("\t", "    ")
        if len(txt) > 86:
            txt = txt[:85] + "..."
        f.text(100, y0 + j * LH + 17, txt, 11.5, 400, anchor="start", c=k, mono=True)
for i in starts[1:]:
    yy = 48 + i * LH - 3
    f.line(30, yy, 870, yy, c="acc", sw=1.5, dash=True)
f.text(870, 48 + 8 * LH + 18, "dashed line: a split, where a date or { starts the next line", 11, 500, anchor="end", c="acc")
f.text(20, 48 + 8 * LH + 18, "134 lines become 80 events", 12, 700, anchor="start")
f.save("ch32-multiline-events")

# 2. DROPMALFORMED: the same 5-row file gives different counts (csv_drift.txt)
data = [("len(collect())", 1, "all columns parsed"), ("count()", 5, "no column is required"),
        ("select(\"id\").count()", 5, "only id is required"), ("count(), pruning off", 3, "field count checked"),
        ("count() after cache()", 1, "all columns parsed")]
f = Fig(820, 250, "DROPMALFORMED counts depend on the query")
f.text(20, 24, "mode=DROPMALFORMED on the same 5 rows (4 of them malformed)", 14, 700, anchor="start")
for i, (q, n, why) in enumerate(data):
    y = 44 + i * 38
    f.text(250, y + 18, q, 12.5, 600, anchor="end", mono=True)
    for r in range(5):
        f.rect(262 + r * 34, y + 4, 28, 22, "ok" if r < n else "ghost", r=4, dash=r >= n)
    f.text(442, y + 20, f"{n} row{'s' if n > 1 else ''}", 13, 700, anchor="start", c="ok" if n == 1 else "acc")
    f.text(520, y + 20, why, 12, 500, anchor="start", c="mute")
f.text(20, 240, "Green: a row that the query keeps. Use PERMISSIVE with _corrupt_record to count bad rows.", 11, 400,
       anchor="start", c="mute", italic=True)
f.save("ch32-dropmalformed")
