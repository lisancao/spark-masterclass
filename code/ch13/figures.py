"""Figures for Chapter 13. Numbers from code/ch13/output/internals.txt (section 3) and compare.txt.

Run from code/:  uv run python ch13/figures.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "code" / "common"))
from svgfig import Fig

# 1. internals.txt, section 3: write all rows, head(5), fare mean
idx = [("sequence", 1.57, 0.20, 0.13, "acc"), ("distributed-sequence (default)", 0.96, 0.68, 0.10, "blue"),
       ("distributed", 0.15, 0.13, 0.10, "ok"), ("index_col='PULocationID'", 0.11, 0.09, 0.09, "purple"),
       ("PySpark DataFrame, no index", 0.08, 0.04, 0.08, "n")]
f = Fig(800, 330, "The cost of each default index")
f.text(20, 28, "Seconds for each operation, by index type, 3,837,248 rows", 15, 700, anchor="start")
cols = [("write all rows (noop)", 1), ("head(5)", 2), ("fare_amount.mean()", 3)]
X0, Y0, BH, G = 250, 60, 14, 6
for c, (title, k) in enumerate(cols):
    x0 = X0 + c * 185
    f.text(x0, Y0, title, 12, 700, anchor="start", c="mute")
    for i, row in enumerate(idx):
        y = Y0 + 14 + i * (3 * BH + 4 * G) / 1.6
        v = row[k]
        w = max(2, 130 * v / 1.6)
        f.rect(x0, y, w, BH + 6, row[4], r=3)
        f.text(x0 + w + 6, y + 15, f"{v:.2f}", 11, 700, anchor="start", c=row[4] if row[4] != "n" else "fg")
for i, row in enumerate(idx):
    y = Y0 + 14 + i * (3 * BH + 4 * G) / 1.6
    f.text(X0 - 12, y + 15, row[0], 12, 600, anchor="end", mono=True)
f.text(20, 318, "median of 5 runs after a warmup, local[*], 16 cores, under the bench lock", 11, 400,
       anchor="start", c="mute", italic=True)
f.save("ch13-index-cost")

# 2. compare.txt
ops = [("groupby mean", 0.052, 0.246, 0.170), ("value_counts", 0.016, 0.163, 0.176),
       ("describe", 0.231, 1.508, 1.345), ("nlargest(5)", 0.076, 1.169, 0.263),
       ("filter + new column", 0.129, 0.202, 0.128)]
eng = [("pandas (data in the driver)", "n"), ("pandas API on Spark", "acc"), ("PySpark DataFrame", "blue")]
f = Fig(800, 340, "Five operations, three APIs")
f.text(20, 28, "Seconds for the same operation, 3,837,248 rows (pandas times exclude its read)", 15, 700, anchor="start")
X0, Y0, PW = 190, 60, 520
for i, (name, *vals) in enumerate(ops):
    y = Y0 + i * 50
    f.text(X0 - 12, y + 24, name, 12, 600, anchor="end", mono=True)
    for j, v in enumerate(vals):
        w = max(2, PW * v / 1.6)
        f.rect(X0, y + j * 14, w, 12, eng[j][1], r=2)
        f.text(X0 + w + 6, y + j * 14 + 10, f"{v:.3f}", 10.5, 600, anchor="start", c=eng[j][1] if eng[j][1] != "n" else "fg")
for j, (lab, k) in enumerate(eng):
    f.rect(X0 + j * 190, 312, 14, 10, k, r=2)
    f.text(X0 + j * 190 + 20, 321, lab, 11.5, 600, anchor="start")
f.save("ch13-compare")
