"""Figures for Chapter 7. Numbers from code/ch07/output/what_cleaning_drops.txt and quickstart.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. Rows that each rule affects (what_cleaning_drops.txt), errors vs partial rows.
rules = [("pickup outside June 2026", 17, "bad", "filter removes"),
         ("fare of zero or less", 16815, "bad", "filter removes"),
         ("distance outside 0.1 to 100 miles", 160940, "bad", "filter removes"),
         ("no passenger count", 1013500, "blue", "keep (not used here)"),
         ("not paid by card (no tip recorded)", 1404377, "acc", "keep, but not for tips")]
f = Fig(860, 300, "Rows that each cleaning rule affects")
f.text(20, 26, "Rows that each rule affects, of 3,837,248", 15, 700, anchor="start")
x0, W = 300, 330
f.rect(x0, 44, W, 236, "ghost", r=0, fill="none", dash=True)
for i, (lab, n, k, act) in enumerate(rules):
    y = 52 + i * 46
    f.text(x0 - 12, y + 19, lab, 13, 500, anchor="end")
    w = max(2, W * n / 3837248)
    f.rect(x0, y + 4, w, 22, k, r=3)
    f.text(x0 + w + 8, y + 20, f"{n:,}  ({100*n/3837248:.1f}%)", 12, 600, anchor="start", c=k)
    f.text(850, y + 20, act, 12, 600, anchor="end", c=k)
f.text(x0 + W, 294, "dashed frame = all rows", 11, 400, anchor="end", c="mute", italic=True)
f.save("ch07-cleaning-rules")

# 2. The ten busiest pickup zones (quickstart.txt).
zones = [("Upper East Side South", 173550, 14.03, 25.3), ("Midtown Center", 156493, 18.35, 24.2),
         ("Upper East Side North", 152687, 14.83, 24.8), ("JFK Airport", 142209, 62.27, 19.0),
         ("Midtown East", 116289, 17.27, 24.3), ("Lincoln Square East", 108791, 15.77, 25.0),
         ("Times Sq/Theatre District", 106540, 21.27, 23.7), ("Penn Station/Madison Sq West", 104614, 18.71, 23.3),
         ("Murray Hill", 99467, 17.4, 24.1), ("LaGuardia Airport", 98621, 46.29, 23.9)]
f = Fig(880, 380, "The ten busiest pickup zones")
f.text(220, 24, "trips", 13, 700, anchor="start")
f.text(560, 24, "avg_fare ($)", 13, 700, anchor="start")
f.text(700, 24, "tip_pct (card trips)", 13, 700, anchor="start")
for i, (z, n, fare, tip) in enumerate(zones):
    y = 40 + i * 32
    air = "Airport" in z
    k = "purple" if air else "blue"
    f.text(210, y + 17, z, 12.5, 600 if air else 500, anchor="end")
    w = 260 * n / 173550
    f.rect(220, y + 4, w, 20, k, r=3)
    f.text(220 + w + 6, y + 18, f"{n:,}", 11.5, 500, anchor="start", c="mute")
    fw = 90 * fare / 62.27
    f.rect(560, y + 4, fw, 20, k, r=3)
    f.text(560 + fw + 6, y + 18, f"{fare:.2f}", 11.5, 500, anchor="start", c="mute")
    # tip dot on a 18..26 scale
    tx = 730 + 130 * (tip - 18) / 8
    f.line(730, y + 14, 860, y + 14, c="grid", sw=1)
    f.circle(tx, y + 14, 6, kind="acc" if tip < 20 else "ok")
    f.text(tx + (12 if tip < 20 else -10), y + 18, f"{tip}%", 11.5, 600, anchor="start" if tip < 20 else "end",
           c="acc" if tip < 20 else "ok")
f.text(730, 372, "18%", 10, 400, anchor="start", c="mute")
f.text(860, 372, "26%", 10, 400, anchor="end", c="mute")
f.rect(20, 356, 12, 12, "blue", r=2); f.text(38, 366, "Manhattan", 11, 500, anchor="start", c="mute")
f.rect(120, 356, 12, 12, "purple", r=2); f.text(138, 366, "Queens airports", 11, 500, anchor="start", c="mute")
f.save("ch07-top-zones")

# 3. How when() without otherwise() gives a conditional sum. Schematic rows.
f = Fig(860, 250, "Conditional sums with when and no otherwise")
f.text(20, 24, "F.sum(F.when(card, F.col(\"tip_amount\")))", 13, 600, anchor="start", mono=True)
rows = [["payment_type", "tip_amount", "fare_amount", "when(card, tip)", "when(card, fare)"],
        ["1 (card)", "3.00", "15.00", "3.00", "15.00"],
        ["2 (cash)", "0.00", "12.00", "null", "null"],
        ["1 (card)", "5.00", "20.00", "5.00", "20.00"],
        ["2 (cash)", "0.00", "30.00", "null", "null"]]
f.table(20, 40, [120, 110, 110, 140, 140], rows, rh=28, mono_cols=(1, 2, 3, 4), bad_rows=(2, 4),
        align=["start", "end", "end", "end", "end"])
f.text(20, 200, "count(*) and avg(fare_amount) still use all 4 rows", 12, 500, anchor="start", c="mute")
f.box(660, 48, 180, 56, "sum = 8.00", "sum ignores null", kind="ok", size=14)
f.box(660, 116, 180, 56, "sum = 35.00", "card fares only", kind="ok", size=14)
f.box(660, 184, 180, 44, "tip_pct = 22.9", kind="acc", size=14)
f.arrow(642, 76, 658, 76); f.arrow(642, 144, 658, 144)
f.note("schematic rows, not from the data")
f.save("ch07-when-sum")
