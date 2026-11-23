"""Chapter 93 figures. Numbers from code/ch93/output/clustering.txt and als.txt.
Run from code/:  uv run python ch93/figures.py [png-check-dir]"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def kmeans_k():
    sil = [(2, 0.579), (3, 0.636), (4, 0.560), (5, 0.540), (6, 0.471), (7, 0.509), (8, 0.460)]
    cost = [492.1, 387.0, 303.1, 255.3, 220.1, 198.8, 182.8]
    f = Fig(900, 340, "KMeans on 128 zones: silhouette and training cost for k from 2 to 8")
    f.text(20, 28, "KMeans on 128 zone profiles: the lowest-cost model of seeds 1-5 for each k", size=14, weight=700)
    X0, X1, Y0, Y1 = 80, 820, 280, 70
    xs = {k: X0 + (k - 2) * (X1 - X0) / 6 for k in range(2, 9)}
    f.line(X0, Y0, X1, Y0)
    for k, x in xs.items():
        f.text(x, Y0 + 20, f"k={k}", size=12, anchor="middle", mono=True)
    # cost as bars (right scale), silhouette as a line (left scale)
    for (k, _), c in zip(sil, cost):
        h = c / 520 * (Y0 - Y1)
        f.rect(xs[k] - 26, Y0 - h, 52, h, c="gr", fill=0.18, rx=2, stroke=0)
        f.text(xs[k], Y0 - 10, f"cost {c:.0f}", size=11, anchor="middle", c="mute", mono=True)
    def sy(s):
        return Y0 - 30 - (s - 0.40) / 0.26 * (Y0 - Y1 - 30)
    pts = " ".join(f"{'M' if i == 0 else 'L'}{xs[k]},{sy(s)}" for i, (k, s) in enumerate(sil))
    f.path(pts, c="go", w=2.6)
    for k, s in sil:
        f.circle(xs[k], sy(s), 5 if k != 3 else 7, c="go")
        f.text(xs[k], sy(s) - 12, f"{s:.3f}", size=12, c="go", weight=700 if k == 3 else 400, mono=True, anchor="middle")
    f.rect(600, 34, 14, 14, c="go", fill=0.9, rx=7, stroke=0)
    f.text(620, 46, "silhouette (higher is better)", size=12)
    f.rect(600, 54, 14, 14, c="gr", fill=0.3, rx=2, stroke=0)
    f.text(620, 66, "training cost (always decreases)", size=12)
    f.text(xs[3], Y0 + 42, "best: k=3, sizes 70, 54, 4", size=12, anchor="middle", c="ok", weight=600)
    return f.save("ch93-kmeans-k", CHECK)


def als():
    rows = [("most popular destinations", 0.144, None), ("own top destinations in train", 0.608, 0.303),
            ("ALS implicit, counts, alpha=1", 0.273, None), ("ALS implicit, counts, alpha=40", 0.069, None),
            ("ALS implicit, log1p(counts), alpha=1", 0.324, 0.311),
            ("ALS implicit, log1p(counts), alpha=40", 0.114, None)]
    f = Fig(900, 330, "NDCG at 10 of destination recommendations, baselines against ALS")
    f.text(20, 28, "NDCG@10: the 10 recommended destinations against the top 10 of the test week", size=14, weight=700)
    f.rect(560, 42, 14, 14, c="si", fill=0.6, rx=2, stroke=0)
    f.text(580, 54, "all 251 pickup zones", size=12)
    f.rect(720, 42, 14, 14, c="go", fill=0.7, rx=2, stroke=0)
    f.text(740, 54, "56 zones, < 100 trips", size=12)
    X0, S = 330, 700
    for i, (name, a, b) in enumerate(rows):
        y = 80 + i * 40
        f.text(X0 - 10, y + 14, name, size=12, anchor="end", c="ink" if "own" in name or "log1p(counts), alpha=1" in name else "mute")
        f.rect(X0, y, a * S, 14, c="si", fill=0.6, rx=2, stroke=0)
        f.text(X0 + a * S + 6, y + 12, f"{a:.3f}", size=11, mono=True)
        if b is not None:
            f.rect(X0, y + 16, b * S, 12, c="go", fill=0.7, rx=2, stroke=0)
            f.text(X0 + b * S + 6, y + 27, f"{b:.3f}", size=11, mono=True, c="go")
    f.line(X0, 70, X0, 320)
    return f.save("ch93-als", CHECK)


if __name__ == "__main__":
    for fn in (kmeans_k, als):
        print(fn())
