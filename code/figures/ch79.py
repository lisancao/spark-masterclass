"""Figures for Chapter 79. Numbers from code/ch79/output/*.txt and the sample files of docs_source.py."""
from pathlib import Path
from svgkit import *

# 1. Which process runs each method (where_it_runs.txt)
s = Svg(780, 330, "The processes of one read from a Python data source, with their PIDs")
s.box(20, 40, 200, 70, "your script", GRAY, 14, sub="pid 617044\nregister(Traced)", subsize=11)
s.box(20, 140, 200, 50, "driver JVM", GRAY, 14)
s.line(120, 110, 120, 138, MUTE, 2)
s.frame(260, 20, 230, 180, "planning workers", MUTE, 13)
s.box(275, 50, 200, 62, "at load()", BLUE, 13, sub="pid 620466\n__init__, schema()", subsize=11, mono=False)
s.box(275, 122, 200, 66, "at the first action", BLUE, 13, sub="pid 621038\nreader(), partitions()", subsize=11)
s.line(220, 160, 273, 85, MUTE, 1.5); s.line(220, 165, 273, 155, MUTE, 1.5)
s.frame(530, 20, 230, 290, "executor workers (one per task)", MUTE, 13)
for i, (p, pid) in enumerate([(0, 621403), (2, 621920), (1, 621919)]):
    s.box(545, 50 + i * 62, 200, 52, f"read(partition {p})", ORANGE, 13, sub=f"pid {pid}", subsize=11)
s.text(645, 250, "the second action ran only these", 11, MUTE)
s.text(645, 265, "tasks again: no new planning", 11, MUTE)
s.path("M475,155 C505,155 505,80 543,76", ORANGE, 1.8)
s.text(375, 220, "the pickled reader and one partition", 11, ACC, "middle", 600)
s.text(375, 234, "go to each task", 11, ACC, "middle", 600)
s.text(20, 240, "No method ran in the script's process.", 12, MUTE, "start", 600)
s.text(20, 258, "PIDs from code/ch79/output/where_it_runs.txt.", 11, MUTE, "start", italic=True)
s.text(20, 274, "They change in each run.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch79-processes.svg")

# 2. Bytes per task, round robin against largest first (docs_source.txt; per-file sizes of the same 26 files)
M = Path("../data/messy")
files = sorted((str(p.resolve()), p.stat().st_size) for f in ["audio", "docs", "events", "images", "logs", "mail", "pdf", "xml"]
               for p in (M / f).glob("**/*") if p.is_file())
rr = [[], [], []]
for i, f in enumerate(files):
    rr[i % 3].append(f)
lf, tot = [[], [], []], [0, 0, 0]
for f in sorted(files, key=lambda f: (-f[1], f[0])):
    i = tot.index(min(tot)); lf[i].append(f); tot[i] += f[1]
assert [sum(x for _, x in g) for g in rr] == [22793, 10884, 30098] and tot == [21266, 21261, 21248]
s = Svg(780, 300, "Bytes per task for the 26 sample files, in two plans")
Y0, SC = 250, 0.0068  # px per byte
for k, (title, groups, x0) in enumerate([("round robin", rr, 60), ("largest first", lf, 440)]):
    s.text(x0 + 140, 22, title, 15, ACC if k else MUTE, "middle", 700)
    for t, g in enumerate(groups):
        x = x0 + t * 95
        y = Y0
        for path, size in g:
            h = size * SC
            big = Path(path).name in ("access.log", "app.log", "silence.wav")
            s.rect(x, y - h, 70, h, ORANGE if big else BLUE, 1, 0, stroke="#ffffff" if h > 3 else None)
            if big and h > 14:
                s.text(x + 35, y - h / 2 + 4, Path(path).name, 10, "#ffffff", "middle", 600)
            y -= h
        b = sum(x for _, x in g)
        s.text(x + 35, y - 8, f"{b:,} B", 12, MUTE, "middle", 700)
        s.text(x + 35, Y0 + 16, f"task {t}", 11, MUTE)
        s.text(x + 35, Y0 + 30, f"{len(g)} files", 10, MUTE)
    s.line(x0 - 10, Y0, x0 + 270, Y0, MUTE, 1, arrow=False)
s.save(FIG + "ch79-balance.svg")
