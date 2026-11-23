"""Figures for Chapter 39. Numbers from code/ch39/output/partitions.txt, pushdown.txt and batch.txt."""
import re
from pathlib import Path

from svgkit import *

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "ch39" / "output"
FIG = str(HERE.parents[1] / "book" / "media" / "figures") + "/"

# 1. Rows per task for four partition plans (partitions.txt)
text = (OUT / "partitions.txt").read_text()
plans = []
for block in text.split("\n== ")[1:]:
    m = re.search(r"rows per task: \[([\d, ]+)\]", block)
    if m:
        plans.append((block.splitlines()[0].strip("= "), [int(x) for x in m.group(1).split(",")]))
labels = {
    "no partition options": "no partition options",
    "partitionColumn=trip_id": "trip_id, bounds 1 to 3,837,248, 8 tasks",
    "bounds from min and max": "pickup time, bounds from min and max, 6 tasks",
    "bounds 2026-06-01": "pickup time, bounds June 1 to July 1, 6 tasks",
    "predicates": "predicates: four week ranges",
}
s = Svg(900, 70 + 78 * len(plans), "Rows that each task read, for five partition plans of the trips table")
total = 3837248
y = 24
for name, rows in plans:
    lab = next((v for k, v in labels.items() if k in name), name)
    s.text(20, y + 14, lab, 14, MUTE, "start", 700)
    x = 20
    W = 845
    for i, r in enumerate(rows):
        w = max(W * r / total, 3)
        col = RED if r > 0.9 * total and len(rows) > 1 else (BLUE if i % 2 == 0 else TEAL)
        s.rect(x, y + 24, w - 2 if w > 4 else w, 30, col, 1, 3)
        if w > 60:
            s.text(x + w / 2, y + 44, f"{r:,}", 12, "#ffffff", "middle", 700)
        x += w
    if any(r < 0.01 * total for r in rows) and len(rows) > 1:
        small = [f"{r:,}" for r in rows if r < 0.01 * total]
        s.text(880, y + 14, "tasks with " + ", ".join(small) + " rows", 12, RED, "end", 700)
    y += 78
s.text(20, y + 6, "Bar width = rows that the task read, out of 3,837,248 (partitions.txt). "
       "One stray trip from 2008 stretched the time range.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch39-ranges.svg")
print("ok")

# 2. What each read path pushed, and the cursor fetches (pushdown.txt)
text = (OUT / "pushdown.txt").read_text()
blocks = {}
for sec in text.split("\n== ")[0:]:
    sec = sec.lstrip("= ")
    title = sec.splitlines()[0]
    for sub in sec.split("\n-- ")[1:]:
        head = sub.splitlines()[0]
        f = re.search(r"cursor fetches of 1000 rows: (\d+)", sub)
        pushed = [ln.strip().split(":")[0] for ln in sub.splitlines() if ln.strip().startswith("Pushed")]
        blocks[(title, head)] = (int(f.group(1)) if f else None, pushed)
rows = [
    ("filter and column pruning", "filter, columns"),
    ("a filter with a function (upper)", "upper(col) = 'Y'"),
    ("aggregate: average fare by payment type", "GROUP BY, AVG"),
    ("limit", "LIMIT 5"),
    ("offset and limit", "OFFSET 10 LIMIT 3"),
    ("top-N: the five highest fares", "ORDER BY ... LIMIT 5"),
]
s = Svg(1000, 90 + 52 * len(rows), "What each read path pushes into PostgreSQL")
s.text(420, 30, "jdbc format (DSv1)", 15, MUTE, "middle", 700)
s.text(800, 30, "JDBC catalog (DSv2)", 15, ACC, "middle", 700)
y = 50
for key, lab in rows:
    s.text(20, y + 26, lab, 14, MUTE, "start", 700, mono=True)
    for col, path in ((420, "jdbc format (DSv1)"), (800, "JDBC catalog (DSv2)")):
        f, pushed = blocks[(key, path)]
        agg_or_more = any(p in ("PushedAggregates", "PushedLimit", "PushedTopN", "PushedOffset") for p in pushed)
        filt = "PushedFilters" in pushed and not agg_or_more
        if key.startswith("a filter with"):
            ok, txt = False, "upper() stays in Spark"
        elif key.startswith("filter"):
            ok, txt = True, "pushed"
        else:
            ok, txt = agg_or_more, ("pushed" if agg_or_more else "in Spark")
        s.box(col - 170, y + 6, 340, 38, f"{txt}: {f:,} fetches", GREEN if ok else GRAY, 13)
    y += 52
s.text(20, y + 20, "A fetch line in the server log = one more round trip of up to 1,000 rows after the first. "
       "3,837 fetches = the whole table.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch39-pushdown.svg")
print("ok2")

# 3. Rows per second by batchsize (batch.txt)
lines = [ln for ln in (OUT / "batch.txt").read_text().splitlines() if re.match(r"batchsize=", ln)]
data = []
for ln in lines:
    m = re.match(r"(.+?)\s{2,}([\d.]+)\s+([\d,]+)\s+(.*)$", ln)
    data.append((m.group(1).strip(), float(m.group(2)), int(m.group(3).replace(",", ""))))
s = Svg(900, 60 + 44 * len(data), "Rows per second into PostgreSQL by batchsize")
X0, SC = 360, 400 / 700000
for t in range(0, 700001, 100000):
    s.line(X0 + t * SC, 20, X0 + t * SC, 24 + 44 * len(data), MUTE, 0.6, dash=True, arrow=False)
    s.text(X0 + t * SC, 38 + 44 * len(data), f"{t // 1000}k", 11, MUTE)
for i, (lab, sec, rps) in enumerate(data):
    yy = 24 + i * 44
    s.text(X0 - 10, yy + 22, lab.replace(", reWriteBatchedInserts=true", " + rewrite"), 13, MUTE, "end", 700, mono=True)
    s.rect(X0, yy + 6, rps * SC, 26, GREEN if "rewrite" in lab or "reWrite" in lab else BLUE, 1, 3)
    s.text(X0 + rps * SC + 8, yy + 24, f"{rps:,}/s ({sec:.2f} s)", 12, MUTE, "start", 700)
s.text(20, 56 + 44 * len(data), "1,000,000 rows, 4 tasks, PostgreSQL 16 in Docker on the same machine, median of 3 writes. "
       "rewrite = reWriteBatchedInserts=true.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch39-batch.svg")
print("ok3")
