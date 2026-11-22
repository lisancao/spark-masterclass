"""Figures for Chapter 68. Numbers from code/ch68/output/*.txt."""
from svgkit import *

# 1. The manifest after each write (writers.txt)
s = Svg(780, 250, "The manifest and the part files after each write of the jsonlog sink")
steps = [("append 1", 200, 3, 1, GREEN, "commit"), ("append 2", 400, 6, 2, GREEN, "commit"),
         ("overwrite", 200, 3, 3, BLUE, "commit, then delete old files"), ("failed append", 200, 3, 3, RED, "abort: staging deleted")]
for i, (lab, rows, files, commits, c, how) in enumerate(steps):
    x = 20 + i * 190
    s.box(x, 20, 170, 34, lab, c, 14)
    s.text(x + 85, 74, how, 11, MUTE)
    s.frame(x, 86, 170, 140, "", MUTE, r=8, dash=False)
    s.box(x + 12, 96, 146, 40, "_MANIFEST.json", GRAY, 12, mono=True, sub=f"rows {rows}, files {files}", subsize=11)
    for k in range(files):
        col, row = k % 3, k // 3
        s.rect(x + 14 + col * 49, 150 + row * 30, 42, 22, BLUE, 1, 3)
        s.text(x + 35 + col * 49, 165 + row * 30, "part", 10, "#ffffff")
    s.text(x + 85, 218, f"commits {commits}", 11, MUTE)
    if i < 3:
        s.line(x + 172, 37, x + 188, 37, MUTE, 2)
s.text(20, 244, "After the failed append, the manifest and the files are the same as after the overwrite.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch68-manifest-states.svg")

# 2. The checkpoint across three runs (streaming.txt)
s = Svg(780, 280, "offsets and commits of the inbox stream in three runs")
X0 = 130
runs = [("run 1\navailableNow", [(0, 3), (1, 1)], None), ("run 2\nrestart", [(2, 3), (3, 1)], None), ("run 3, after\ndeleting commits/3", [(3, 1)], 3)]
for b in range(4):
    x = X0 + b * 150
    s.text(x + 65, 22, f"batch {b}", 13, MUTE, "middle", 700)
s.text(X0 - 12, 50, "offsets/N", 12, MUTE, "end", 600, mono=True)
s.text(X0 - 12, 80, "commits/N", 12, MUTE, "end", 600, mono=True)
for b in range(4):
    x = X0 + b * 150
    s.box(x, 34, 130, 24, f"offsets/{b}", GRAY, 11, mono=True)
    s.box(x, 64, 130, 24, f"commits/{b}", GRAY, 11, mono=True, dash=b == 3, stroke=ORANGE if b == 3 else None)
s.text(X0 + 3 * 150 + 65, 104, "deleted before run 3", 11, ACC, "middle", 600)
for r, (lab, batches, replay) in enumerate(runs):
    y = 124 + r * 42
    s.text(X0 - 12, y + 12, lab, 11, MUTE, "end", 600)
    for b, n in batches:
        x = X0 + b * 150
        if replay is not None:
            s.box(x, y, 130, 30, f"{n} file, replay skipped", ORANGE, 11)
        else:
            s.box(x, y, 130, 30, f"{n} file" + ("s" if n > 1 else ""), GREEN, 12)
s.text(20, 272, "The sink saw batch 3 two times with the same batch ID. It skipped the second commit, and the sink has 8 rows from 8 files.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch68-checkpoint.svg")
