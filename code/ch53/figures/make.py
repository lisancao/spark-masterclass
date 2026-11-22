"""Chapter 53 figures. Numbers from code/ch53/output/fare_pipeline.txt and estimators.txt."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def mae():
    rows = [("1", 52463, 3.0, 2.33), ("NULL", 18106, 6.81, 6.29), ("2", 1732, 5.91, 1.38), ("5", 346, 19.79, 24.19),
            ("3", 225, 6.16, 16.31), ("4", 161, 22.29, 25.46), ("99", 16, 4.04, 4.73)]
    f = Fig(900, 330, "Mean absolute error by rate code, linear regression and boosted trees")
    f.text(20, 26, "Mean absolute error by rate code (test set)", size=14, weight=700)
    f.rect(560, 14, 14, 14, c="si", fill=0.5, rx=2, stroke=0)
    f.text(580, 26, "linear regression", size=12)
    f.rect(710, 14, 14, 14, c="go", fill=0.7, rx=2, stroke=0)
    f.text(730, 26, "gradient-boosted trees", size=12)
    X0, BASE, S, W = 70, 270, 8.0, 46
    f.line(X0 - 10, BASE, 880, BASE, c="line")
    for i, (rc, n, lin, gbt) in enumerate(rows):
        x = X0 + i * 118
        f.rect(x, BASE - lin * S, W, lin * S, c="si", fill=0.5, rx=2, stroke=0)
        f.rect(x + W + 4, BASE - gbt * S, W, gbt * S, c="go", fill=0.7, rx=2, stroke=0)
        f.text(x + W / 2, BASE - lin * S - 5, f"{lin}", size=11, anchor="middle", mono=True)
        f.text(x + W * 1.5 + 4, BASE - gbt * S - 5, f"{gbt}", size=11, anchor="middle", mono=True)
        f.text(x + W + 2, BASE + 18, f"rate {rc}", size=12, anchor="middle", weight=600)
        f.text(x + W + 2, BASE + 34, f"{n:,} trips", size=11, anchor="middle", c="mute")
    f.text(X0 + 2 * 118 + W + 2, BASE - 5.91 * S - 22, "JFK flat fare", size=11, anchor="middle", c="ok", weight=600)
    return f.save("ch53-mae", CHECK)


def vector():
    f = Fig(900, 260, "How a row becomes a feature vector")
    f.text(20, 26, "One row: trip_distance = 2.5 (example value), rate_code = '2'", size=14, weight=700)
    f.box(20, 50, 170, 50, "rate_code '2'", "a string category", c="br", size=13)
    f.line(190, 75, 228, 75, arrow=True)
    f.box(232, 50, 190, 50, "StringIndexer", "'2' has index 1", c="go", size=13)
    f.line(422, 75, 460, 75, arrow=True)
    f.box(464, 50, 200, 50, "OneHotEncoder", "(6,[1],[1.0])", c="go", size=13)
    f.line(664, 75, 702, 75, arrow=True)
    f.box(706, 50, 175, 50, "VectorAssembler", "7 slots", c="si", size=13)
    names = ["trip_distance", "rate_vec_1", "rate_vec_2", "rate_vec_5", "rate_vec_3", "rate_vec_4", "rate_vec_99"]
    vals = ["2.5", "0", "1", "0", "0", "0", "0"]
    for i, (n, v) in enumerate(zip(names, vals)):
        x = 20 + i * 123
        f.rect(x, 140, 115, 36, c="si" if i == 0 else ("go" if v == "1" else "gr"), fill=0.2 if v != "0" else 0.06, rx=3)
        f.text(x + 57, 164, v, size=15, anchor="middle", mono=True, weight=700)
        f.text(x + 57, 194, n, size=11, anchor="middle", mono=True, c="mute")
        f.text(x + 57, 132, f"slot {i}", size=10, anchor="middle", c="mute")
    f.text(20, 226, "The slot names come from the metadata of the vector column (ml_attr). Rate code 1 has index 0,",
           size=12, c="mute")
    f.text(20, 244, "so its one-hot slot is rate_vec_1. With dropLast=True, the seventh category (index 6) has no slot.",
           size=12, c="mute")
    return f.save("ch53-vector", CHECK)


if __name__ == "__main__":
    for fn in (mae, vector):
        print(fn())
