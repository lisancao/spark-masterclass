"""Figures for Chapter 12. Numbers from code/ch12/output/keyvalue.txt and partitions.txt.

Run from code/:  uv run python ch12/figures.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "code" / "common"))
from svgfig import Fig

# 1. keyvalue.txt: median, min-max and shuffle write for the three operations
ops = [("reduceByKey", 0.59, 0.57, 0.74, 0.02, "ok"),
       ("aggregateByKey", 0.75, 0.65, 0.88, 0.02, "blue"),
       ("groupByKey", 1.01, 0.85, 1.08, 11.76, "acc")]
f = Fig(800, 300, "Total fare per zone over 3,837,248 pairs, three ways")
f.text(20, 28, "Total fare for each zone over 3,837,248 cached pairs", 15, 700, anchor="start")
L, PW = 150, 230
for panel, (x0, title, unit, vmax) in enumerate(((L, "shuffle write", "MiB", 12), (L + PW + 170, "time per run", "s", 1.2))):
    f.text(x0, 62, title, 13, 700, anchor="start", c="mute")
    for i, (name, med, lo, hi, mib, k) in enumerate(ops):
        y = 80 + i * 62
        if panel == 0:
            f.text(x0 - 12, y + 22, name, 13, 600, anchor="end", mono=True)
        v = mib if panel == 0 else med
        w = max(2, PW * v / vmax)
        f.rect(x0, y + 6, w, 26, k, r=4)
        lab = f"{v:.2f} {unit}"
        if panel == 1:
            f.line(x0 + PW * lo / vmax, y + 19, x0 + PW * hi / vmax, y + 19, c="fg", sw=1.5)
            f.line(x0 + PW * lo / vmax, y + 12, x0 + PW * lo / vmax, y + 26, c="fg", sw=1.5)
            f.line(x0 + PW * hi / vmax, y + 12, x0 + PW * hi / vmax, y + 26, c="fg", sw=1.5)
            f.text(x0 + PW * hi / vmax + 8, y + 24, lab, 12, 700, anchor="start", c=k)
        else:
            f.text(x0 + w + 8, y + 24, lab, 12, 700, anchor="start", c=k)
    f.line(x0, 80, x0, 80 + 3 * 62, c="line", sw=1)
f.text(20, 278, "bars: median of 5 runs after a warmup (lines: min to max), local[*], 16 cores, under the bench lock",
       11, 400, anchor="start", c="mute", italic=True)
f.save("ch12-keyvalue")

# 2. partitions.txt: rows per partition after partitionBy(8), and where four busy zones went
rows = [223057, 467721, 728887, 299179, 713132, 424708, 508386, 472178]
keys = {4: "132 (JFK)", 2: "138 (LGA)", 1: "161", 5: "237"}
f = Fig(800, 330, "Rows per partition after partitionBy(8)")
f.text(20, 28, "partitionBy(8): partition = portable_hash(zone) % 8, and the hash of an int is the int", 14, 700,
       anchor="start")
X0, Y0, PH, BW, G = 80, 60, 200, 64, 22
f.line(X0 - 10, Y0 + PH, X0 + 8 * (BW + G), Y0 + PH, c="line")
for v in (0, 250000, 500000, 750000):
    y = Y0 + PH - PH * v / 750000
    f.line(X0 - 10, y, X0 + 8 * (BW + G), y, c="grid", sw=1)
    f.text(X0 - 14, y + 4, f"{v // 1000}k", 11, 400, anchor="end", c="mute")
for i, v in enumerate(rows):
    x = X0 + i * (BW + G)
    h = PH * v / 750000
    k = "acc" if i in keys else "n"
    f.rect(x, Y0 + PH - h, BW, h, k, r=3)
    f.text(x + BW / 2, Y0 + PH - h - 6, f"{v:,}", 11, 600, c="fg")
    f.text(x + BW / 2, Y0 + PH + 18, f"p{i}", 12, 700, mono=True)
    if i in keys:
        f.text(x + BW / 2, Y0 + PH + 36, keys[i], 11, 600, c="acc")
f.text(20, 318, "zone 132 % 8 = 4, 138 % 8 = 2, 161 % 8 = 1, 237 % 8 = 5. Partition 4 also holds zones 4, 12, 20, 28 ...",
       11, 400, anchor="start", c="mute", italic=True)
f.save("ch12-partitions")
