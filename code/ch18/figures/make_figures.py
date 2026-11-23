"""Figures for Chapter 18. Numbers from code/ch18/output/windows.txt (the 8-row sales table)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

sales = [("Bronx", "06-01", 5), ("Bronx", "06-02", 15), ("Bronx", "06-05", 15),
         ("Queens", "06-01", 10), ("Queens", "06-02", 30), ("Queens", "06-03", 30), ("Queens", "06-04", 20), ("Queens", "06-06", 50)]
tot = {"Bronx": 35, "Queens": 140}

# 1. groupBy against a window
f = Fig(860, 330, "groupBy against a window function")
f.text(20, 24, "8 input rows", 14, 700, anchor="start")
rows = [["borough", "day", "trips"]] + [[b, d, str(t)] for b, d, t in sales]
f.table(20, 34, [80, 60, 50], rows, rh=28, size=12, align=["start", "start", "end"],
        hi_rows=(1, 2, 3), hi_kind="blue", ok_rows=())
f.text(300, 24, "groupBy(\"borough\")", 12.5, 600, anchor="start", mono=True)
f.table(300, 34, [80, 70], [["borough", "sum"], ["Bronx", "35"], ["Queens", "140"]], rh=28, size=12,
        align=["start", "end"], hi_rows=(1,), hi_kind="blue")
f.text(300, 140, "2 rows: one per group", 12, 600, anchor="start", c="mute")
f.text(520, 24, "F.sum(\"trips\").over(w_all)", 12.5, 600, anchor="start", mono=True)
rows = [["borough", "trips", "total", "share"]] + [[b, str(t), str(tot[b]), f"{t / tot[b]:.2f}"] for b, d, t in sales]
f.table(520, 34, [80, 50, 60, 60], rows, rh=28, size=12, align=["start", "end", "end", "end"], hi_rows=(1, 2, 3),
        hi_kind="blue")
f.text(520, 304, "8 rows: each row keeps its columns", 12, 600, anchor="start", c="mute")
f.arrow(212, 90, 292, 90, c="line"); f.arrow(212, 180, 512, 180, c="line")
f.save("ch18-groupby-window")

# 2. ROWS against RANGE on a calendar (section 5)
f = Fig(860, 330, "ROWS against RANGE frames on dates")
f.text(20, 24, "sum(trips) for the current row, 2 PRECEDING", 14, 700, anchor="start")
X0, DW = 120, 100
for d in range(1, 7):
    f.text(X0 + (d - 1) * DW + DW / 2, 50, f"June {d}", 12, 600, c="mute")
    f.line(X0 + (d - 1) * DW, 58, X0 + (d - 1) * DW, 300, c="grid", sw=1)
f.line(X0 + 6 * DW, 58, X0 + 6 * DW, 300, c="grid", sw=1)
lanes = [("Bronx", [(1, 5), (2, 15), (5, 15)], 110), ("Queens", [(1, 10), (2, 30), (3, 30), (4, 20), (6, 50)], 240)]
for name, pts, y in lanes:
    f.text(X0 - 12, y + 5, name, 13, 700, anchor="end")
    for d, t in pts:
        cur = (name, d) in (("Bronx", 5), ("Queens", 6))
        f.box(X0 + (d - 1) * DW + 25, y - 16, 50, 32, str(t), kind="acc" if cur else "n", size=13)
# Bronx 5 June: ROWS covers 1,2,5 ; RANGE covers 3..5
y = 110
f.rect(X0 + 15, y - 30, 4 * DW + 70, 60, "acc", r=10, fill="none", dash=True)
f.text(X0 + 15, y - 36, "ROWS 2 PRECEDING: 3 rows, 5 days, sum 35", 12, 700, anchor="start", c="acc")
f.rect(X0 + 2 * DW + 8, y + 22, 3 * DW - 16, 14, "blue", r=4)
f.text(X0 + 2 * DW + 8, y + 52, "RANGE 2 DAYS PRECEDING: June 3 to 5, sum 15", 12, 700, anchor="start", c="blue")
# Queens 6 June: ROWS covers 3,4,6 =100 ; RANGE covers 4..6 = 70
y = 240
f.rect(X0 + 2 * DW + 15, y - 30, 3 * DW + 70 - 0, 60, "acc", r=10, fill="none", dash=True)
f.text(X0 + 2 * DW + 15, y - 36, "ROWS: June 3, 4, 6, sum 100", 12, 700, anchor="start", c="acc")
f.rect(X0 + 3 * DW + 8, y + 22, 3 * DW - 16, 14, "blue", r=4)
f.text(X0 + 3 * DW + 8, y + 52, "RANGE: June 4 to 6, sum 70", 12, 700, anchor="start", c="blue")
f.text(840, 320, "orange box: the current row", 11, 500, anchor="end", c="acc")
f.save("ch18-rows-range")
