"""Figures for Chapter 83. Numbers from code/ch83/output/app.txt and crossover.txt."""
import math
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "output"
app = (OUT / "app.txt").read_text()
kib = re.search(r"Arrow IPC \(([\d.]+) KiB\)", app).group(1)
rows = re.search(r"Polars: (\d+) rows", app).group(1)
cross = [l.split() for l in (OUT / "crossover.txt").read_text().splitlines()[1:]]
six = next(c for c in cross if c[0] == "6")

# 1. The architecture of the app
f = Fig(1000, 440, "The taxi app: local work in the process, heavy work on Spark")
# the server, on the left
f.rect(20, 90, 250, 260, "acc", r=12)
f.text(145, 124, "Spark Connect server", 16, 700, c="acc")
for i, (t, wgt) in enumerate([("Spark 4.3.0, 16 cores", 500), ("reads 6 months of Parquet", 500),
                              (f"{int(six[2]):,} trips", 700), ("aggregates by zone and hour", 500),
                              (f"{int(rows):,} rows back", 700)]):
    f.text(145, 166 + i * 30, t, 13, wgt, c="acc")
# the app, on the right
f.rect(410, 40, 570, 380, "blue", r=12)
f.text(426, 64, "taxi-app: one Rust binary, no JVM", 15, 700, anchor="start", c="blue")
L, R, W = 430, 710, 250
f.box(L, 84, W, 70, "spark_connect", "DataFrame API: builds the plan", kind="n", size=14, mono=True)
f.box(L, 182, W, 70, "to_arrow()", f"Arrow IPC bytes, {kib} KiB", kind="acc", size=14, mono=True)
f.box(L, 280, W, 70, "FileReader (arrow-rs 60)", "IPC bytes to RecordBatch", kind="n", size=13)
f.box(R, 84, W, 70, "zone lookup CSV", "265 rows, read by Polars", kind="n", size=14)
f.box(R, 182, W, 70, "Polars DataFrame", "IpcReader: joins, shares", kind="ok", size=14)
f.box(R, 280, W, 70, "DuckDB view", "SQL with QUALIFY", kind="ok", size=14)
f.arrow(L + W / 2, 252, L + W / 2, 280)
f.arrow(L + W, 217, R, 217)
f.arrow(L + W, 315, R, 315)
f.arrow(R + W / 2, 154, R + W / 2, 182, "")
f.text(R + W / 2 + 22, 172, "join", 11, 500, c="mute")
f.text(695, 390, "interactive questions: milliseconds, no round trip", 12, 600, c="ok")
# the wire
f.arrow(L, 119, 270, 150, "", c="acc")
f.text(340, 112, "plan", 12, 700, c="acc")
f.text(340, 127, "(protobuf)", 11, 500, c="acc")
f.arrow(270, 290, L, 217, "", c="ok")
f.text(340, 286, "ArrowBatch", 12, 700, c="ok")
f.text(340, 301, "messages", 11, 500, c="ok")
f.text(20, 434, "The sizes come from app.txt. The positions are schematic.", 11, 400, anchor="start", c="mute", italic=True)
f.save("ch83-architecture")

# 2. The crossover: one question on Spark against pull once + ask locally
f = Fig(940, 380, "Spark per question against pull once and ask locally")
f.text(20, 24, "Median of 3 runs (ms, log scale). Spark: one question. Pull: raw rows to Polars, once. Local: one question.",
       13, 700, anchor="start")
x0, y0, w, h = 80, 50, 600, 260
lo, hi = math.log10(3), math.log10(3000)
Y = lambda v: y0 + h - (math.log10(v) - lo) / (hi - lo) * h  # noqa: E731
for v in (3, 10, 30, 100, 300, 1000, 3000):
    f.line(x0, Y(v), x0 + w, Y(v), c="grid", sw=1)
    f.text(x0 - 8, Y(v) + 4, str(v), 11, 400, anchor="end", c="mute")
n = len(cross)
X = lambda i: x0 + 30 + i * (w - 60) / (n - 1)  # noqa: E731
for i, c in enumerate(cross):
    f.text(X(i), y0 + h + 18, f"{c[0]} {c[1]}", 11, 600)
    f.text(X(i), y0 + h + 32, f"{int(c[2]) / 1e6:.1f}M rows", 10, 400, c="mute")
series = [("spark", 4, "acc"), ("pull", 5, "purple"), ("local", 6, "ok")]
for name, col, kind in series:
    pts = [(X(i), Y(float(c[col]))) for i, c in enumerate(cross)]
    f.path(pts, c=kind, sw=2.2, head=False)
    for x, y in pts:
        f.circle(x, y, 3.5, kind)
f.text(x0 + w + 10, Y(float(cross[-1][4])) + 16, "Spark, one question", 12, 700, anchor="start", c="acc")
f.text(x0 + w + 10, Y(float(cross[-1][5])) + 4, "pull the raw rows", 12, 700, anchor="start", c="purple")
f.text(x0 + w + 10, Y(float(cross[-1][6])) - 4, "Polars, one question", 12, 700, anchor="start", c="ok")
be = ", ".join(f"{c[0]} {c[1]}: {' '.join(c[7:])}" for c in cross)
f.text(20, 362, "Questions after which one pull pays: " + be, 10.5, 500, anchor="start", c="mute")
f.save("ch83-crossover")
