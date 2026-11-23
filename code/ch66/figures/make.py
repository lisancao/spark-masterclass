"""Chapter 66 figures. Numbers from code/ch66/output/indexes.txt; the S-curves are the formula 1 - (1 - s^r)^b."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def axes(f, x0, y0, w, h, xlab, ylab, xt, yt, ylo=0.0):
    f.line(x0, y0, x0 + w, y0, c="line")
    f.line(x0, y0, x0, y0 - h, c="line")
    for v in xt:
        x = x0 + v / 100 * w
        f.line(x, y0, x, y0 + 4, c="line")
        f.text(x, y0 + 17, f"{v:g}%", size=11, anchor="middle", c="mute")
    for v in yt:
        y = y0 - (v - ylo) / (1 - ylo) * h
        f.line(x0 - 4, y, x0, y, c="line")
        f.line(x0, y, x0 + w, y, c="line", w=0.5, dash="2 4")
        f.text(x0 - 8, y + 4, f"{v:g}", size=11, anchor="end", c="mute")
    f.text(x0 + w / 2, y0 + 36, xlab, size=12, anchor="middle", c="mute")
    f.text(x0, y0 - h - 10, ylab, size=12, c="mute")


def tradeoff():
    brp = [(0.05, 1, 23.7, 0.905), (0.05, 4, 49.5, 0.976), (0.2, 1, 65.5, 0.976), (0.2, 4, 93.1, 1.0),
           (0.5, 4, 97.7, 1.0)]
    ivf = [(1, 2.0, 0.424), (2, 3.9, 0.591), (4, 7.7, 0.765), (8, 14.7, 0.913), (16, 25.1, 0.988), (64, 100.0, 1.0)]
    f = Fig(900, 380, "Recall against work for LSH on the images and IVF on 20,000 vectors")
    # left: BRP LSH
    x0, y0, w, h = 60, 300, 360, 230
    f.text(20, 26, "BucketedRandomProjectionLSH, 42 images", size=14, weight=700)
    axes(f, x0, y0, w, h, "work: candidate pairs, % of all pairs", "recall@1", [0, 25, 50, 75, 100],
         [0.9, 0.95, 1.0], ylo=0.88)
    for bl, t, wk, rc in brp:
        x, y = x0 + wk / 100 * w, y0 - (rc - 0.88) / 0.12 * h
        f.circle(x, y, 6, c="si" if t == 1 else "go")
        off = {(0.05, 1): (8, -6), (0.05, 4): (-8, -10), (0.2, 1): (8, 16), (0.2, 4): (-8, 18), (0.5, 4): (-4, -10)}[(bl, t)]
        lab = "0.2 or 0.5, 1t" if (bl, t) == (0.2, 1) else ("0.5 or 1.0, 4t" if (bl, t) == (0.5, 4) else f"{bl}, {t}t")
        f.text(x + off[0], y + off[1], lab, size=11, mono=True, anchor="end" if off[0] < 0 else "start")
    f.circle(x0 + 150, 218, 5, c="si"); f.text(x0 + 160, 222, "1 table", size=11)
    f.circle(x0 + 220, 218, 5, c="go"); f.text(x0 + 230, 222, "4 tables", size=11)
    f.text(20, 372, "Labels: bucketLength and tables (t). Two settings with the same result share a point.", size=11, c="mute")
    # right: IVF
    x0 = 520
    f.text(480, 26, "IVF, 64 lists, 20,000 vectors", size=14, weight=700)
    axes(f, x0, y0, w - 20, h, "work: vectors scored, % of brute force", "recall@10", [0, 25, 50, 75, 100],
         [0.4, 0.6, 0.8, 1.0], ylo=0.35)
    pts = []
    for npb, wk, rc in ivf:
        x, y = x0 + wk / 100 * (w - 20), y0 - (rc - 0.35) / 0.65 * h
        pts.append((x, y))
        f.circle(x, y, 5, c="ok")
        f.text(x + (-8 if npb == 64 else 8), y + 16, f"nprobe {npb}", size=11, mono=True, anchor="end" if npb == 64 else "start")
    f.path("M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts), c="ok", w=1.5)
    return f.save("ch66-tradeoff", CHECK)


def scurve():
    cfg = [(100, 1, "si", "100 x 1: 92.10%"), (25, 4, "ok", "25 x 4: 2.50%"), (20, 5, "go", "20 x 5: 1.60%"),
           (10, 10, "bad", "10 x 10: 0.46%")]
    f = Fig(900, 340, "Probability that a pair becomes a candidate, for four choices of bands and rows")
    f.text(20, 26, "P(candidate) = 1 - (1 - s^r)^b, and the candidates that each choice made", size=14, weight=700)
    x0, y0, w, h = 70, 290, 520, 230
    f.line(x0, y0, x0 + w, y0, c="line"); f.line(x0, y0, x0, y0 - h, c="line")
    for v in (0, 0.2, 0.4, 0.6, 0.8, 1.0):
        x = x0 + v * w
        f.text(x, y0 + 17, f"{v:g}", size=11, anchor="middle", c="mute")
        y = y0 - v * h
        f.text(x0 - 8, y + 4, f"{v:g}", size=11, anchor="end", c="mute")
        f.line(x0, y, x0 + w, y, c="line", w=0.5, dash="2 4")
    f.text(x0 + w / 2, y0 + 36, "Jaccard similarity s of a pair", size=12, anchor="middle", c="mute")
    f.text(x0, y0 - h - 10, "P(candidate)", size=12, c="mute")
    xs = x0 + 0.7 * w
    f.line(xs, y0, xs, y0 - h, c="ink", w=1.4, dash="5 4")
    f.text(xs - 6, y0 - 12, "target: s >= 0.7 (435 pairs)", size=11, anchor="end")
    for b, r, c, lab in cfg:
        pts = []
        for i in range(101):
            s = i / 100
            p = 1 - (1 - s ** r) ** b
            pts.append((x0 + s * w, y0 - p * h))
        f.path("M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts), c=c, w=2.2)
    f.text(620, 70, "b x r: candidates, % of all pairs", size=12, weight=600)
    for i, (b, r, c, lab) in enumerate(cfg):
        y = 96 + i * 26
        f.line(620, y - 4, 650, y - 4, c=c, w=3)
        f.text(658, y, lab, size=12, mono=True)
    f.text(620, 214, "recall at s >= 0.7: 1.000 for the first", size=12, c="mute")
    f.text(620, 230, "three, 0.830 for 10 x 10", size=12, c="mute")
    return f.save("ch66-scurve", CHECK)


if __name__ == "__main__":
    for fn in (tradeoff, scurve):
        print(fn())
