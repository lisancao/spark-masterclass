"""Figures for Chapter 64. Numbers from code/ch64/output/*.txt."""
from svgkit import *

# 1. Average of ratios against the real rate (metric_view.txt)
reg = [("metro", 0.0787, 6370), ("urban", 0.099, 2519), ("suburban", 0.1218, 974), ("rural", 0.3775, 99), ("remote", 0.515, 32)]
s = Svg(780, 330, "Regional conversion rates, the by-hand average and the MEASURE() result")
X0, Y0, H = 70, 270, 400   # H px for rate 1.0 -> 0.515 = 206 px
s.text(X0, 20, "Bar height: conversion rate. Bar width: active users of the region (MEASURE() at the region grain).", 12, MUTE, "start")
# left: proportional widths; right: equal widths (how the by-hand average weights them)
tot = sum(r[2] for r in reg)
W = 400
x = X0
for name, rate, users in reg:
    w = W * users / tot
    s.rect(x, Y0 - rate * H, max(w - 1, 1.5), rate * H, BLUE, 1)
    x += w
s.text(X0 + W / 2, Y0 + 18, "weighted by users (the real mix)", 12, MUTE, "middle", 600)
s.text(X0 + W * 6370 / tot / 2, Y0 - 0.0787 * H / 2 + 4, "metro 0.0787, 6,370 users", 11, "#ffffff", "middle", 600)
s.text(X0 + W - 4, Y0 - 0.515 * H - 6, "remote 0.515, 32 users", 11, MUTE, "end")
X1, bw = 520, 48
for i, (name, rate, users) in enumerate(reg):
    xx = X1 + i * bw
    s.rect(xx, Y0 - rate * H, bw - 4, rate * H, GRAY, 1)
    s.text(xx + bw / 2 - 2, Y0 + 14, name, 10, MUTE)
s.text(X1 + 2.5 * bw, Y0 + 32, "equal weight (the by-hand average)", 12, MUTE, "middle", 600)
for val, c, lab, x1, x2 in [(0.0923, GREEN, "MEASURE() 0.0923", X0, X0 + W), (0.2384, RED, "by hand 0.2384", X1 - 6, X1 + 5 * bw)]:
    y = Y0 - val * H
    s.line(x1, y, x2, y, c, 2.5, dash=True, arrow=False)
    s.text(x1 + 4, y - 8, lab, 13, c, "start", 700)
s.line(X0, Y0, X0 + W, Y0, MUTE, 1, arrow=False); s.line(X1 - 6, Y0, X1 + 5 * bw, Y0, MUTE, 1, arrow=False)
s.save(FIG + "ch64-average-of-ratios.svg")

# 2. Which check stops which flow (dryrun.txt)
s = Svg(780, 316, "Five versions of the gold flow and the check that stops each one")
cols = ["register flows", "resolve graph", "run", "trusted number"]
CX = [300, 420, 540, 670]
for c, x in zip(cols, CX):
    s.text(x, 26, c, 13, ACC if c == "trusted number" else MUTE, "middle", 700)
s.text(360, 44, "dry-run, no data read", 11, MUTE, "middle", italic=True)
s.line(250, 50, 475, 50, MUTE, 1, arrow=False)
rows = [("gold_typo.py", 1, "TABLE_OR_VIEW_NOT_FOUND"),
        ("gold_column.py", 1, "UNRESOLVED_COLUMN"),
        ("gold_conf.py", 0, "SESSION_MUTATION_..."),
        ("gold_epoch.py", 3, "561 days in year 58384"),
        ("gold_ok.py", 4, "5 days in June 2026")]
for i, (f, stop, msg) in enumerate(rows):
    y = 64 + i * 44
    s.text(20, y + 22, f, 13, MUTE, "start", 600, mono=True)
    end = CX[min(stop, 3)]
    s.line(200, y + 17, (end - 50) if stop < 4 else 740, y + 17, GREEN if stop == 4 else MUTE, 2, arrow=stop == 4)
    for j, x in enumerate(CX):
        if j < stop:
            s.circle(x, y + 17, 7, GREEN)
    if stop < 4:
        s.box(end - 50, y + 4, 100 if stop < 3 else 110, 26, "stopped", RED if stop < 3 else ORANGE, 12)
        s.text(end + (58 if stop < 3 else 64), y + 22, msg if stop < 3 else "", 11, MUTE, "start", mono=True)
    if stop == 3:
        s.text(CX[2], y + 40, msg + ": the run passed", 11, ACC, "middle", 600)
    if stop == 4:
        s.text(CX[2], y + 40, msg, 11, MUTE, "middle")
s.text(20, 308, "green dot: the check passed. Only a number from outside the pipeline stops gold_epoch.py.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch64-checks.svg")
