"""Chapter 40 figures. Numbers from code/ch40/output/resolve.txt and components.txt."""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def plan_growth():
    f = Fig(900, 340, "Plan size in each round of an iterative loop, with and without a cut")
    left = [("no cut", "bad", [359, 1045, 2417, 5161]), ("localCheckpoint()", "ok", [1, 1, 1, 1])]
    right = [("nothing", "gr", [16, 28, 40, 52, 64]), ("cache()", "bad", [54, 163, 381, 817, 1689, 3433]),
             ("localCheckpoint()", "ok", [1] * 6), ("checkpoint()", "si", [1] * 6)]

    def panel(x0, title, ylab, series, rounds):
        y0, w, h = 280, 280, 210
        f.text(x0 - 40, 26, title, size=14, weight=700)
        f.line(x0, y0, x0 + w, y0, c="line"); f.line(x0, y0, x0, y0 - h, c="line")
        Y = lambda v: y0 - math.log10(v) / 4 * h
        for v in (1, 10, 100, 1000, 10000):
            f.line(x0, Y(v), x0 + w, Y(v), c="line", w=0.5, dash="2 4")
            f.text(x0 - 8, Y(v) + 4, f"{v:,}", size=11, anchor="end", c="mute")
        X = lambda r: x0 + (r - 1) / (rounds - 1) * (w - 20) + 10
        for r in range(1, rounds + 1):
            f.text(X(r), y0 + 17, str(r), size=11, anchor="middle", c="mute")
        f.text(x0 + w / 2, y0 + 34, "round", size=12, anchor="middle", c="mute")
        f.text(x0, y0 - h - 8, ylab, size=12, c="mute")
        for k, (name, c, vals) in enumerate(series):
            pts = [(X(i + 1), Y(v) - (k - 1.5) * 2 if v == 1 else Y(v)) for i, v in enumerate(vals)]
            f.path("M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts), c=c, w=2.2)
            for x, y in pts:
                f.circle(x, y, 3.5, c=c)
            lx, ly = pts[-1]
            if vals[-1] == 1:
                f.text(lx + 8, ly - 10 - 14 * (k - (len(series) - 2 if len(series) > 2 else 1)), f"{name}: 1", size=11, c=c, weight=600)
            else:
                f.text(lx + 8, ly + 4, f"{name}: {vals[-1]:,}", size=11, c=c, weight=600)

    panel(80, "Label propagation (Step 5)", "analyzed plan lines (log scale)", left, 4)
    panel(500, "Star rounds, four ways (Step 9)", "optimized plan lines (log scale)", right, 6)
    f.text(20, 332, "The 'nothing' loop printed five rounds in the output. The two cuts give a plan of one line in each round.",
           size=11, c="mute")
    return f.save("ch40-plan-growth", CHECK)


def wrong_edge():
    f = Fig(900, 270, "One wrong edge joins two people into a cluster of six")
    f.text(20, 26, "Largest cluster: 6 records, two people per the answer key (E057 and E058)", size=14, weight=700)
    A = [(120, 90), (60, 190), (180, 190)]
    B = [(420, 90), (360, 190), (480, 190)]
    for grp, c, lab in ((A, "si", "E057: 3 records"), (B, "go", "E058: 3 records")):
        for i in range(3):
            for j in range(i + 1, 3):
                f.line(*grp[i], *grp[j], c=c, w=2)
        for x, y in grp:
            f.circle(x, y, 16, c=c, fill=0.3, stroke=2)
        f.text(grp[0][0], 248, lab, size=12, anchor="middle", c=c, weight=600)
    f.line(180, 190, 360, 190, c="bad", w=3, dash="6 4")
    f.text(270, 180, "one wrong match", size=12, anchor="middle", c="bad", weight=600)
    f.text(560, 80, "6 records imply 15 pairs:", size=13, weight=600)
    f.text(560, 104, "3 + 3 = 6 correct pairs inside the two people", size=12)
    f.text(560, 126, "3 x 3 = 9 wrong pairs across the two people", size=12, c="bad")
    f.text(560, 164, "precision of the matcher:      0.991", size=12, mono=True)
    f.text(560, 186, "precision after clustering:    0.921", size=12, mono=True)
    f.text(560, 226, "Record positions: schematic.", size=11, c="mute", italic=True)
    return f.save("ch40-wrong-edge", CHECK)


def sweep():
    pts = [(0.7, 156, 0.673, 1.0), (0.8, 115, 0.913, 1.0), (0.9, 106, 0.991, 1.0), (1.0, 102, 0.990, 0.962),
           (1.1, 96, 1.0, 0.914), (1.3, 89, 1.0, 0.848)]
    f = Fig(900, 300, "Precision and recall of the evidence score at six thresholds")
    f.text(20, 26, "Threshold sweep on the evidence score (105 true pairs)", size=14, weight=700)
    x0, y0, w, h = 80, 250, 520, 190
    lo = 0.6
    f.line(x0, y0, x0 + w, y0, c="line"); f.line(x0, y0, x0, y0 - h, c="line")
    X = lambda t: x0 + (t - 0.65) / 0.7 * w
    Y = lambda v: y0 - (v - lo) / (1 - lo) * h
    for v in (0.6, 0.7, 0.8, 0.9, 1.0):
        f.line(x0, Y(v), x0 + w, Y(v), c="line", w=0.5, dash="2 4")
        f.text(x0 - 8, Y(v) + 4, f"{v:g}", size=11, anchor="end", c="mute")
    for t, n, p, r in pts:
        f.text(X(t), y0 + 17, f">= {t:g}", size=11, anchor="middle", c="mute")
        f.text(X(t), y0 + 32, f"{n} matches", size=10, anchor="middle", c="mute")
    for key, c, name in ((2, "si", "precision"), (3, "go", "recall")):
        pp = [(X(q[0]), Y(q[key])) for q in pts]
        f.path("M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pp), c=c, w=2.2)
        for x, y in pp:
            f.circle(x, y, 4, c=c)
    f.line(X(0.9), y0, X(0.9), y0 - h, c="ok", w=1.4, dash="5 4")
    f.text(640, 90, "precision", size=13, c="si", weight=600)
    f.text(640, 112, "recall", size=13, c="go", weight=600)
    f.text(640, 150, "At 0.9: precision 0.991, recall 1.000.", size=12)
    f.text(640, 170, "The one false match is two people", size=12, c="mute")
    f.text(640, 188, "with the name Mei Chen.", size=12, c="mute")
    return f.save("ch40-sweep", CHECK)


if __name__ == "__main__":
    for fn in (plan_growth, wrong_edge, sweep):
        print(fn())
