"""Figures for Chapter 11. Numbers from code/ch11/output/formats.txt and bytes.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. The same million trips in five formats (formats.txt)
data = [("csv", 116.5, 0.167, 0.488, 16, "n"), ("csv.gz", 20.5, 0.993, 1.823, 1, "bad"),
        ("json", 454.1, 0.293, 0.697, 16, "n"), ("orc", 19.5, 0.102, 0.687, 5, "blue"),
        ("parquet", 21.0, 0.048, 0.695, 5, "ok")]
f = Fig(860, 290, "One million trips in five formats")
f.text(20, 24, "One million trips: size, and time of avg(fare_amount)", 14, 700, anchor="start")
f.text(120, 52, "size on disk (MB)", 12, 700, anchor="start", c="mute")
f.text(500, 52, "avg(fare) (s), median of 7", 12, 700, anchor="start", c="mute")
for i, (fm, mb, t1, t2, tasks, k) in enumerate(data):
    y = 66 + i * 40
    f.text(110, y + 18, fm, 13, 700, anchor="end", mono=True)
    w = 260 * mb / 454.1
    f.rect(120, y + 4, max(w, 2), 22, k, r=3)
    f.text(120 + max(w, 2) + 6, y + 20, f"{mb}", 12, 600, anchor="start", c="mute")
    w2 = 250 * t1 / 0.993
    f.rect(500, y + 4, w2, 22, k, r=3)
    f.text(500 + w2 + 6, y + 20, f"{t1:.3f}  ({tasks} task{'s' if tasks > 1 else ''})", 12, 600, anchor="start", c="mute")
f.text(20, 282, "csv.gz is small but not splittable: one task reads it. Parquet and ORC read one column only.", 11, 400,
       anchor="start", c="mute", italic=True)
f.save("ch11-formats")

# 2. The PNG header and the IHDR check (bytes.txt)
f = Fig(860, 270, "The first 24 bytes of a PNG file")
f.text(20, 24, "The first 24 bytes of a PNG file, as substring(content, start, n) sees them", 14, 700, anchor="start")
fields = [(1, 8, "signature", "89 50 4E 47 0D 0A 1A 0A", "acc"), (9, 4, "chunk length", "00 00 00 0D", "n"),
          (13, 4, "chunk name", "49 48 44 52 = IHDR", "blue"), (17, 4, "width", "00 00 00 20 = 32", "ok"),
          (21, 4, "height", "00 00 00 20 = 32", "ok")]
X0, BW = 20, 34
for s, n, name, val, k in fields:
    x = X0 + (s - 1) * BW
    f.box(x, 44, n * BW - 4, 44, name, kind=k, size=12)
    f.text(x + (n * BW - 4) / 2, 106, val, 11, 600, c=k, mono=True)
    f.text(x + 2, 124, f"bytes {s}-{s + n - 1}", 10.5, 400, anchor="start", c="mute")
f.text(20, 160, "gradient_32x32.png: IHDR at bytes 13-16, so png_ok = true, width 32, height 32", 12, 600, anchor="start", c="ok")
f.text(20, 190, "corrupt.png: the right signature, then the text \"not really a png\"", 12, 600, anchor="start", c="bad")
f.box(20, 202, 8 * BW - 4, 30, "89504E470D0A1A0A", kind="acc", size=11, mono=True)
f.box(20 + 8 * BW, 202, 16 * BW - 4, 30, "not really a png", kind="bad", size=12, mono=True)
f.text(20 + 8 * BW, 252, "bytes 13-16 = 7265616C (\"real\"), not IHDR: png_ok = false. Without the check: width 1819877473.", 11.5, 500, anchor="start", c="bad")
f.save("ch11-png-header")
