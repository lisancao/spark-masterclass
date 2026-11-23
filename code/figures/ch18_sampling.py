"""Figure for Chapter 18 (TABLESAMPLE). Values from code/ch18/output/sampling.txt section 2 and 4."""
from pathlib import Path
from svgkit import *

OUT = str(Path(__file__).resolve().parents[2] / "book" / "media" / "figures") + "/"
s = Svg(820, 330, "Rows that the Parquet scan returned against rows that the sample kept")
rows = [("TABLESAMPLE (1 PERCENT)", 3837248, 38394, "Sample"),
        ("TABLESAMPLE (BUCKET 1 OUT OF 100)", 3837248, 38069, "Sample"),
        ("TABLESAMPLE (1000 ROWS)", 16384, 1000, "CollectLimit"),
        ("WHERE payment_type = 3, then 10 PERCENT", 3145728, 1211, "Filter, Sample")]
X0, MAXW = 330, 420
s.text(20, 26, "rows read by the scan (gray) and rows kept (orange), linear scale", 14, MUTE, "start", 600)
for j, (label, scan, kept, op) in enumerate(rows):
    y = 50 + j * 66
    s.text(X0 - 12, y + 16, label, 13, MUTE, "end", 600)
    s.text(X0 - 12, y + 34, f"plan: {op}", 12, MUTE, "end")
    w = max(MAXW * scan / 3837248, 3)
    s.rect(X0, y + 2, w, 20, GRAY, 0.9, 3)
    s.text(X0 + w + 6, y + 17, f"{scan:,}", 12, MUTE, "start")
    k = max(MAXW * kept / 3837248, 3)
    s.rect(X0, y + 26, k, 14, ORANGE, 1, 3)
    s.text(X0 + k + 6, y + 38, f"{kept:,}", 12, ACC, "start", 600)
s.text(410, 318, "A percent or bucket sample reads every row. ROWS is a limit, and it stops early.", 13, ACC)
s.save(OUT + "ch18-tablesample.svg")
print("ok")
