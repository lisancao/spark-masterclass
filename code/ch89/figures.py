"""Figures for Chapter 89. Run: python3 ch89/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig


def start_positions():
    # output/k2_start.txt: end offsets after each delivery, and the first offset of each option
    ends = {0: (309, 618, 925), 1: (355, 672, 1018), 2: (196, 371, 598), 3: (135, 245, 383)}
    marks = {  # option -> first offset read in each partition (end - rows read)
        "earliest": {0: 0, 1: 0, 2: 0, 3: 0},
        "JSON offsets": {0: 100, 1: 0, 2: 598, 3: 300},
        "startingTimestamp=t2": {0: 925 - 616, 1: 1018 - 663, 2: 598 - 402, 3: 383 - 248},
        "ByTimestamp (t3, t2, t2, t3)": {0: 925 - 307, 1: 1018 - 663, 2: 598 - 402, 3: 383 - 138},
        "latest": {0: 925, 1: 1018, 2: 598, 3: 383},
    }
    kinds = {"earliest": "ok", "JSON offsets": "blue", "startingTimestamp=t2": "accent",
             "ByTimestamp (t3, t2, t2, t3)": "warn", "latest": "red"}
    f = Fig(860, 470, "Where each starting option begins in the topic trips")
    f.text(20, 22, "Topic trips, 4 partitions, three deliveries (k2_start.txt). A mark is the first offset that a new query reads.",
           size=12.5, anchor="start", color="mute")
    X0, X1 = 70, 820
    x = lambda o: X0 + o / 1018 * (X1 - X0)
    fills = ["mutef", "soft", "fgf"]
    for p in range(4):
        y = 60 + p * 84
        f.text(X0 - 12, y + 18, f"p{p}", size=14, anchor="end", weight=700)
        lo = 0
        for d, hi in enumerate(ends[p]):
            f.rect(x(lo), y, x(hi) - x(lo), 26, fill=fills[d], stroke="line", rx=2)
            if p == 0:
                f.text((x(lo) + x(hi)) / 2, y - 8, f"delivery {d + 1}", size=11.5, color="mute")
            lo = hi
        f.text(x(ends[p][2]) + 6, y + 18, str(ends[p][2]), size=11.5, anchor="start", color="mute")
        for i, (opt, m) in enumerate(marks.items()):
            mx = x(m[p])
            k = {"ok": "ok", "blue": "blue", "accent": "acc", "warn": "warn", "red": "red"}[kinds[opt]]
            f.path(f"M{mx:.1f},{y + 28 + i * 10} l-5,9 l10,0 z", color=k, fill=k)
    ly = 400
    for i, opt in enumerate(marks):
        k = {"ok": "ok", "blue": "blue", "accent": "acc", "warn": "warn", "red": "red"}[kinds[opt]]
        lx = [40, 140, 270, 450, 680][i]
        f.path(f"M{lx},{ly + 2} l-5,9 l10,0 z", color=k, fill=k)
        f.text(lx + 10, ly + 11, opt, size=11.5, anchor="start", color=k)
    f.note("p2 JSON mark is -1 (latest). p3 offset 500 is past the end of the partition: data loss error.", y=455)
    f.save("ch89-start-positions")


def tasks():
    # output/k4_admission.txt: offset ranges per task, default and minPartitions=10
    default = [(0, 0, 924), (1, 0, 1017), (2, 0, 597), (3, 0, 382)]
    split = [(1, 0, 253), (1, 254, 507), (1, 508, 762), (1, 763, 1017), (0, 0, 307), (0, 308, 615),
             (0, 616, 924), (3, 0, 382), (2, 0, 298), (2, 299, 597)]
    f = Fig(860, 360, "Spark tasks for one batch of a 4-partition topic")
    f.text(20, 22, "One bar for each Kafka partition. Each box is one Spark task and its offsets (k4_admission.txt).",
           size=12.5, anchor="start", color="mute")
    X0, X1 = 200, 820
    x = lambda o: X0 + o / 1018 * (X1 - X0)
    kinds = {0: "blue", 1: "accent", 2: "ok", 3: "warn"}
    for col, (label, rows, y0) in enumerate([("default: 4 tasks", default, 60), ("minPartitions=10: 10 tasks", split, 200)]):
        f.text(20, y0 + 6, label, size=13.5, anchor="start", weight=700)
        for p in range(4):
            y = y0 + 16 + p * 28
            f.text(X0 - 10, y + 15, f"kafka p{p}", size=12, anchor="end", color="mute")
            for t, (pp, lo, hi) in enumerate(rows):
                if pp != p:
                    continue
                f.box(x(lo), y, x(hi + 1) - x(lo) - 2, 22, f"task {t}", "", kind=kinds[p], size=11, rx=3)
    f.note("Partition 3 (383 records) stays one task. The large partitions get more tasks.", y=348)
    f.save("ch89-tasks")


if __name__ == "__main__":
    start_positions()
    tasks()
