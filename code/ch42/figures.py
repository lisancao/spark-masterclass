"""Figures for Chapter 42. Run: python3 ch42/figures.py (from code/). Numbers from output/*.txt."""
import math
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig


def latency():
    # output/r2_latency.txt: medians over three runs (the table in Step 4)
    rows = [("pass-through", "micro-batch", 110, 374), ("pass-through", "Real-time Mode", 1, 4),
            ("running count", "micro-batch", 276, 759), ("running count", "Real-time Mode", 28, 195)]
    f = Fig(820, 290, "Latency of the two engines, Kafka to Kafka")
    f.text(20, 20, "p50 and p99 latency in ms, medians over three runs, log scale (r2_latency.txt)", size=13, anchor="start", color="mute")
    X0, X1 = 270, 680
    lx = lambda v: X0 + math.log10(v) / 3 * (X1 - X0)
    for v in (1, 10, 100, 1000):
        f.line(lx(v), 44, lx(v), 230, color="line", sw=0.8, dash="2 4")
        f.text(lx(v), 244, f"{v} ms", size=11.5, color="mute")
    for i, (q, e, p50, p99) in enumerate(rows):
        y = 64 + i * 44
        f.text(X0 - 14, y, f"{q}, {e}", size=13, anchor="end", weight=600 if e.startswith("Real") else 400)
        k = "ok" if e.startswith("Real") else "blue"
        f.line(lx(p50), y, lx(p99), y, color="ok" if k == "ok" else "blue", sw=2)
        f.circle(lx(p50), y, 6, fill="ok" if k == "ok" else "blue")
        f.circle(lx(p99), y, 6, fill="none", stroke="ok" if k == "ok" else "blue", sw=2)
        f.text(lx(p99) + 12, y, f"p50 {p50}, p99 {p99}", size=12, anchor="start")
    f.circle(X0, 270, 5, fill="mute"); f.text(X0 + 10, 270, "p50", size=12, anchor="start")
    f.circle(X0 + 70, 270, 5, fill="none", stroke="mute", sw=2); f.text(X0 + 80, 270, "p99", size=12, anchor="start")
    f.save("ch42-latency")


def foreach():
    # output/r3_foreach_default.txt and r3_foreach_64.txt: record i sent at i s; latency after its send
    dflt = [11846, 11833, 11517, 10518, 9517, 8515, 7517, 6517, 5517, 4517]
    b64 = [121, 1865, 1003, 4005, 3005, 2005, 1004, 6230, 5230, 4230]
    f = Fig(860, 300, "When the records reach the Python foreach writer")
    f.text(20, 20, "One record a second into a Python foreach writer, 20 s Real-time batches (r3_foreach_default.txt, r3_foreach_64.txt)", size=12.5, anchor="start", color="mute")
    X0, X1 = 170, 830
    x = lambda s: X0 + s / 15 * (X1 - X0)
    for s in range(0, 16, 1):
        f.line(x(s), 46, x(s), 244, color="line", sw=0.6, dash="1 4")
        if s % 2 == 0:
            f.text(x(s), 258, f"{s} s", size=11, color="mute")
    for row, (name, lat, col) in enumerate([("spark.buffer.size\n65536 (default)", dflt, "red"), ("spark.buffer.size\n64", b64, "warn")]):
        y0 = 70 + row * 100
        f.text(X0 - 14, y0 + 18, name, size=12.5, anchor="end", mono=True)
        for i, l in enumerate(lat):
            sent, got = i, i + l / 1000
            f.circle(x(sent), y0, 4, fill="blue")
            f.line(x(sent), y0, x(got), y0 + 40, color=col, sw=1.1)
            f.circle(x(got), y0 + 40, 4.5, fill=col)
        f.text(x(0) - 6, y0 - 12, "sent", size=11, anchor="start", color="blue")
        f.text(x(0) - 6, y0 + 56, "reached Python", size=11, anchor="start", color=col)
    f.text(X0, 284, "Each line joins the send time of a record with the time when process() got it.", size=12, anchor="start")
    f.save("ch42-foreach")


if __name__ == "__main__":
    latency(); foreach()
