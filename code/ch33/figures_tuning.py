"""Figures for the Chapter 33 tuning steps (serialization, GC, locality). Run: python3 ch33/figures_tuning.py
from code/. Numbers are parsed from output/*.txt."""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig

OUT = Path(__file__).resolve().parent / "output"


def locality():
    txt = (OUT / "locality.txt").read_text()
    blocks = re.split(r"\n(?=spark\.locality|3s, legacy)", txt)[1:]
    runs = []
    for b in blocks:
        head = b.splitlines()[0]
        label = head.split(":")[0]
        took = float(re.search(r"took ([\d.]+) s", head).group(1))
        cached = {k: int(v) for k, v in re.findall(r"'(\d+)': (\d+)", re.search(r"\{[^}]*\}", head).group(0))}
        big = max(cached, key=cached.get)
        tasks = [(float(t), e, int(p), loc) for t, e, p, loc in
                 re.findall(r"\+\s*([\d.]+) s\s+executor (\d+)\s+partition (\d+)\s+(\w+)", b)]
        runs.append((label, took, big, tasks))
    W, H = 920, 70 + len(runs) * 120 + 40
    f = Fig(W, H, "Tasks of job 2 on two executors, for four locality settings")
    f.text(20, 22, "Job 2: eight 1 s tasks on a cache. Executor A holds 7 cached partitions, executor B holds 1 (locality.txt)",
           size=13, anchor="start", color="mute")
    x0, x1, tmax = 130, 880, 8.0
    sx = lambda t: x0 + t / tmax * (x1 - x0)
    for s in range(0, 9):
        f.line(sx(s), 44, sx(s), H - 40, color="line", dash="2 4")
        f.text(sx(s), 40, f"{s} s", size=10.5, color="mute")
    for i, (label, took, big, tasks) in enumerate(runs):
        y = 58 + i * 120
        f.text(x0, y + 8, f"{label}:  job 2 took {took:.1f} s", size=12.5, anchor="start", weight=600, mono=True)
        for lane, name in ((0, "A (7 blocks)"), (1, "B (1 block)")):
            ly = y + 22 + lane * 40
            f.text(x0 - 10, ly + 15, name, size=12, anchor="end", color="mute")
        for t, e, p, loc in tasks:
            lane = 0 if e == big else 1
            ly = y + 22 + lane * 40
            kind = "blue" if loc == "PROCESS_LOCAL" else "warn"
            f.box(sx(t) + 1, ly, sx(t + 1.0) - sx(t) - 2, 30, f"p{p}", "", kind=kind, size=11.5, rx=4, weight=500)
    ly = H - 24
    f.box(x0 + 60, ly - 8, 14, 14, kind="blue", rx=2); f.text(x0 + 80, ly, "PROCESS_LOCAL (block in this executor)", size=12, anchor="start")
    f.box(x0 + 390, ly - 8, 14, 14, kind="warn", rx=2); f.text(x0 + 410, ly, "NODE_LOCAL (block read from the other executor)", size=12, anchor="start")
    f.save("ch33-locality")


def serialization():
    txt = (OUT / "serialization.txt").read_text()
    rows = []
    for l in txt.splitlines():
        m = re.match(r"(.+?)\s+objects\s+([\d.]+) MiB \| MEMORY_ONLY_SER\s+([\d.]+) MiB\s+([\d.]+) s \| RDD shuffle\s+([\d.]+) MiB\s+([\d.]+) s", l)
        if m:
            rows.append((m.group(1).strip(), *map(float, m.groups()[1:])))
    W, H = 900, 70 + len(rows) * 58 + 40
    f = Fig(W, H, "Cache and shuffle sizes for each serializer")
    f.text(20, 22, "2,000,000 Trip objects: deserialized cache, serialized cache and RDD shuffle, in MiB (serialization.txt)",
           size=13, anchor="start", color="mute")
    x0, x1 = 230, 880
    vmax = max(max(r[1], r[2], r[4]) for r in rows)
    sx = lambda v: (x1 - x0) * v / vmax
    for i, (label, obj, ser, tser, shuf, tshuf) in enumerate(rows):
        y = 56 + i * 58
        f.text(x0 - 12, y + 22, label, size=12.5, anchor="end")
        for j, (v, kind, name) in enumerate([(obj, "mute", "objects"), (ser, "blue", "MEMORY_ONLY_SER"), (shuf, "accent", "RDD shuffle")]):
            yy = y + j * 15
            f.box(x0, yy, max(2, sx(v)), 13, kind=kind, rx=2)
            f.text(x0 + sx(v) + 6, yy + 7, f"{v:.1f}", size=11, anchor="start", color="mute")
    ly = H - 22
    x = x0
    for kind, t in [("mute", "objects in the heap (MEMORY_ONLY)"), ("blue", "MEMORY_ONLY_SER"), ("accent", "RDD shuffle (compressed)")]:
        f.box(x, ly - 7, 14, 14, kind=kind, rx=2); f.text(x + 20, ly, t, size=12, anchor="start"); x += 30 + len(t) * 7
    f.save("ch33-serialization")


if __name__ == "__main__":
    locality()
    if (OUT / "serialization.txt").exists() and "objects" in (OUT / "serialization.txt").read_text():
        serialization()
