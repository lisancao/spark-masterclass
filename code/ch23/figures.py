"""Figures for Chapter 23. Run: python3 ch23/figures.py (from code/). Numbers from output/*.txt."""
import math
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit import Fig


def crossover():
    # output/crossover.txt: on/off ratio of medians
    rows = [1000, 10000, 100000, 300000, 1000000, 2000000, 3837248]
    agg = [0.51, 0.56, 0.53, 0.44, 0.65, 0.74, 1.45]
    win = [0.64, 0.73, 0.72, 0.83, 1.38, 2.31, 5.52]
    f = Fig(820, 400, "On/off time ratio of single-task execution by file size")
    f.text(20, 20, "Time with the setting on, divided by time with it off (crossover.txt). Below 1: one task is faster.", size=13, anchor="start", color="mute")
    X0, X1, Y0, Y1 = 90, 780, 350, 50
    lx = lambda r: X0 + (math.log10(r) - 3) / (math.log10(3837248) - 3) * (X1 - X0)
    ymax = 6.0
    ly = lambda v: Y0 - (v / ymax) * (Y0 - Y1)
    for v in (0, 1, 2, 3, 4, 5, 6):
        f.line(X0, ly(v), X1, ly(v), color="line", sw=0.8 if v != 1 else 1.6, dash=None if v == 1 else "2 4")
        f.text(X0 - 10, ly(v), f"{v}", size=11.5, anchor="end", color="mute")
    f.text(X0 + 8, ly(1) - 9, "same time", size=11.5, anchor="start", color="mute")
    f.rect(X0, ly(1), X1 - X0, ly(0) - ly(1), fill="okf", stroke="none", rx=0, opacity=0.6)
    for r, lab in zip(rows, ["1K", "10K", "100K", "300K", "1M", "2M", "3.8M"]):
        f.text(lx(r), Y0 + 18, lab, size=11.5, color="mute")
    f.text((X0 + X1) / 2, Y0 + 38, "rows in the file (log scale)", size=12, color="mute")
    for vals, col, name in [(agg, "blue", "aggregate"), (win, "acc", "window")]:
        d = "M" + " L".join(f"{lx(r):.1f},{ly(v):.1f}" for r, v in zip(rows, vals))
        f.path(d, color=col, sw=2.4)
        for r, v in zip(rows, vals):
            f.circle(lx(r), ly(v), 4, fill=col)
        dy = -12 if name == "window" else 30
        f.text(lx(rows[-1]) - 10, ly(vals[-1]) + dy, f"{name} {vals[-1]}", size=12.5, anchor="end", color=col, weight=700)
    f.save("ch23-crossover")


def jobs():
    # output/plans_aqe.txt: the UI records of one collect()
    f = Fig(820, 230, "Jobs of one collect() with single-task execution off and on")
    f.text(20, 20, "Zones by borough, sorted: one collect() (plans_aqe.txt)", size=13, anchor="start", color="mute")
    f.text(20, 64, "setting off", size=14, anchor="start", weight=700)
    for i in range(4):
        f.box(130 + i * 120, 40, 108, 50, f"job {i + 1}", "1 stage, 1 task", kind="paper", size=13, subsize=11)
    f.text(625, 58, "4 jobs, 4 stages, 4 tasks", size=13, anchor="start", weight=600)
    f.text(625, 76, "shuffle write 1,172 B, 1,933 ms", size=12.5, anchor="start", color="mute")
    f.text(20, 154, "setting on", size=14, anchor="start", weight=700)
    f.box(130, 130, 108, 50, "job 1", "1 stage, 1 task", kind="ok", size=13, subsize=11)
    f.text(625, 148, "1 job, 1 stage, 1 task", size=13, anchor="start", weight=600)
    f.text(625, 166, "shuffle write 0 B, 340 ms", size=12.5, anchor="start", color="mute")
    f.note("The counts and the durations are from the UI records in plans_aqe.txt.", y=214)
    f.save("ch23-jobs")


def arrowcache():
    # output/arrow_cache.txt
    rows = [("default", 102.1, 114, 1.7), ("Arrow", 149.5, 132, 1.3), ("Arrow, zstd", 35.6, 157, 1.5)]
    f = Fig(760, 186, "Memory of the cache for each serializer")
    f.text(20, 20, "June trips, 6 columns, df.cache(): memory, fill time and a groupBy from the cache (arrow_cache.txt)", size=13, anchor="start", color="mute")
    x0, mw = 140, 360
    for i, (n, mib, q, fill) in enumerate(rows):
        y = 44 + i * 46
        f.text(x0 - 12, y + 15, n, size=13.5, anchor="end", mono=True)
        f.box(x0, y, mw * mib / 149.5, 30, kind="accent" if n == "default" else "blue", rx=4)
        f.text(x0 + mw * mib / 149.5 + 10, y + 15, f"{mib} MiB", size=13.5, anchor="start", weight=700)
        f.text(x0 + mw * mib / 149.5 + 90, y + 15, f"fill {fill} s, query {q} ms", size=12, anchor="start", color="mute")
    f.save("ch23-arrowcache")


if __name__ == "__main__":
    crossover(); jobs(); arrowcache()
