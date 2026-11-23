"""Figures for Chapter 11. Numbers from code/ch11/output/strategies.txt and skew.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. Which rows each join type keeps (strategies.txt section 1)
outcomes = ["a_null", "a1", "a2 + b2", "a3 + b3", "a3 + b3x", "b4", "b_null"]
kinds = ["n", "acc", "ok", "ok", "ok", "blue", "n"]
keep = {"inner": [2, 3, 4], "left": [0, 1, 2, 3, 4], "right": [2, 3, 4, 5, 6], "full": [0, 1, 2, 3, 4, 5, 6],
        "left_semi": [2, 3], "left_anti": [0, 1]}
f = Fig(870, 300, "The rows that each join type keeps")
f.text(20, 24, "a: k = 1, 2, 3, NULL      b: k = 2, 3, 3, 4, NULL", 13, 600, anchor="start", mono=True)
X0, CW = 150, 90
for j, o in enumerate(outcomes):
    f.box(X0 + j * CW + 4, 40, CW - 8, 34, o, kind=kinds[j], size=12, mono=True)
f.text(X0 + 2.5 * CW + CW / 2 + 0 * CW, 92, "matches (k = 2, 3)", 11, 600, c="ok")
f.text(X0 + CW, 92, "only in a", 11, 600, c="acc")
f.text(X0 + 5.5 * CW, 92, "only in b", 11, 600, c="blue")
for i, (jt, ks) in enumerate(keep.items()):
    y = 112 + i * 28
    f.text(X0 - 12, y + 14, jt, 13, 600, anchor="end", mono=True)
    f.line(X0, y + 10, X0 + 7 * CW, y + 10, c="grid", sw=1)
    for j in range(7):
        cx = X0 + j * CW + CW / 2
        if j in ks:
            semi = jt == "left_semi" and j == 3
            f.circle(cx, y + 10, 7, kind=kinds[j] if kinds[j] != "n" else "line" if False else kinds[j])
    f.text(X0 + 7 * CW + 12, y + 14, f"{len(ks)} rows", 12, 600, anchor="start", c="mute")
f.text(20, 292, "NULL keys never match with ==. left_semi keeps each left row one time and no columns of b.", 11, 400,
       anchor="start", c="mute", italic=True)
f.save("ch11-join-types")

# 2. The same join with each strategy (strategies.txt section 3)
data = [("BROADCAST", "BroadcastHashJoin", 0.17, "ok"), ("SHUFFLE_HASH", "ShuffledHashJoin", 0.54, "blue"),
        ("MERGE", "SortMergeJoin", 0.75, "blue"), ("SHUFFLE_REPLICATE_NL", "CartesianProduct", 8.61, "bad")]
f = Fig(820, 230, "trips x zones with each join hint")
f.text(20, 24, "trips x zones, count per borough: median of 3 after a warmup", 14, 700, anchor="start")
for i, (h, op, s, k) in enumerate(data):
    y = 44 + i * 42
    f.text(250, y + 14, h, 12.5, 700, anchor="end", mono=True)
    f.text(250, y + 30, op, 11, 500, anchor="end", c="mute")
    w = 480 * s / 8.61
    f.rect(262, y + 6, max(w, 2), 24, k, r=4)
    f.text(262 + max(w, 2) + 8, y + 23, f"{s:.2f} s", 13, 700, anchor="start", c=k)
f.text(20, 220, "BroadcastHashJoin does not move the trips. The other three shuffle or copy both sides.", 11, 400,
       anchor="start", c="mute", italic=True)
f.save("ch11-strategy-times")

# 3. Skewed join stage: median and max task time (skew.txt sections 2 and 3)
data = [("AQE skew join off", 1390, 4919, 3.71, "bad"), ("AQE skew join on", 679, 2114, 2.73, "acc"),
        ("salting, skew join off", 1730, 1887, 2.26, "ok")]
f = Fig(820, 240, "Task times of the join stage with one hot key")
f.text(20, 24, "Join stage task time, 30 M fact rows, 40% with k = 0", 14, 700, anchor="start")
X0, W = 220, 500
def px(ms): return X0 + W * ms / 5000
for v in range(0, 5001, 1000):
    f.line(px(v), 40, px(v), 182, c="grid", sw=1)
    f.text(px(v), 198, f"{v:,} ms", 11, 400, c="mute")
for i, (lab, med, mx, q, k) in enumerate(data):
    y = 64 + i * 44
    f.text(X0 - 12, y + 4, lab, 12.5, 600, anchor="end")
    f.text(X0 - 12, y + 20, f"query {q:.2f} s", 11, 500, anchor="end", c="mute")
    f.line(px(med), y, px(mx), y, c=k, sw=3)
    f.circle(px(med), y, 6, kind="n")
    f.circle(px(mx), y, 6, kind=k)
    f.text(px(mx) + 10, y + 4, f"max {mx:,}", 12, 700, anchor="start", c=k)
f.circle(240, 222, 5, kind="n"); f.text(250, 226, "median task", 11, 500, anchor="start", c="mute")
f.circle(350, 222, 5, kind="bad"); f.text(360, 226, "slowest task", 11, 500, anchor="start", c="mute")
f.save("ch11-skew-tasks")
