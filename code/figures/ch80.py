"""Figures for Chapter 80. Numbers from code/ch80/output/*.txt and code/ch80/shopdb.py."""
from svgkit import *

# 1. tuple, Row and Arrow batch (arrow_timing.txt)
s = Svg(780, 210, "Seconds to read 2,000,000 rows from a Python source, by what read() yields")
rows = [("tuple", 1.29, 1.04, 1.45, BLUE), ("Row", 3.32, 2.38, 3.62, GRAY), ("RecordBatch", 0.22, 0.20, 0.24, GREEN)]
X0, SC = 150, 120
for t in range(0, 5):
    s.line(X0 + t * SC, 20, X0 + t * SC, 150, MUTE, 0.6, dash=True, arrow=False)
    s.text(X0 + t * SC, 166, f"{t} s", 11, MUTE)
for i, (lab, med, lo, hi, c) in enumerate(rows):
    y = 30 + i * 40
    s.text(X0 - 12, y + 17, lab, 14, MUTE, "end", 700, mono=True)
    s.rect(X0, y + 4, med * SC, 20, c, 1, 3)
    s.line(X0 + lo * SC, y + 14, X0 + hi * SC, y + 14, MUTE, 1.5, arrow=False)
    s.line(X0 + lo * SC, y + 8, X0 + lo * SC, y + 20, MUTE, 1.5, arrow=False)
    s.line(X0 + hi * SC, y + 8, X0 + hi * SC, y + 20, MUTE, 1.5, arrow=False)
    s.text(X0 + hi * SC + 10, y + 19, f"median {med} s", 13, MUTE, "start", 700)
s.text(X0, 192, "5 runs each after a warm-up, local[*] on 16 cores, 4 partitions. Lines: min to max.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch80-arrow-timing.svg")

# 2. Stride ranges against quantile ranges (database_source.txt; ids from shopdb.py)
s = Svg(780, 300, "Four partitions of the orders table: equal key ranges against equal row counts")
X0, W = 40, 700
kmin, kmax = 1, 100006
def kx(k): return X0 + (k - kmin) / (kmax - kmin) * W
s.text(X0, 22, "min..max stride ranges: the key space from 1 to 100006 in four equal parts", 13, MUTE, "start", 700)
y = 40
cnt = [1201, 0, 0, 6]
for i in range(4):
    x = X0 + i * W / 4
    s.box(x + 2, y, W / 4 - 4, 34, f"{cnt[i]} rows", RED if cnt[i] == 0 else (ORANGE if cnt[i] > 1000 else GRAY), 13)
s.rect(kx(1), y + 44, max(kx(1201) - kx(1), 2), 10, BLUE, 1)
s.rect(kx(100001), y + 44, 4, 10, BLUE, 1)
s.text(kx(1), y + 70, "ids 1 to 1201", 11, MUTE, "start")
s.text(kx(100006), y + 70, "six legacy ids from 100001", 11, MUTE, "end")
s.text(X0, 150, "quantile ranges: split points 302, 604, 906 from the index (the six legacy ids at the end)", 13, ACC, "start", 700)
y = 168
qs = [1, 302, 604, 906, 1207]
cnt = [301, 302, 302, 302]
for i in range(4):
    x = X0 + (qs[i] - 1) / 1206 * W
    w = (qs[i + 1] - qs[i]) / 1206 * W
    s.box(x + 2, y, w - 4, 34, f"{cnt[i]} rows", GREEN, 13)
s.text(X0, y + 64, "rows in key order (1207 rows)", 11, MUTE, "start")
s.rect(X0, y + 40, W, 6, BLUE, 1)
s.text(X0 + W, y + 64, "the last range ends with the legacy ids", 11, MUTE, "end")
s.text(X0, 270, "A task that reads 1201 rows sets the time of the stage. The quantile plan costs a few queries during planning.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch80-key-ranges.svg")
