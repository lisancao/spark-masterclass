"""Chapter 43 figures. Numbers from code/ch43/output/attempts.txt, connect_determinism.txt and fragile_job.txt."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def attempts():
    f = Fig(900, 300, "Task attempts under local[4,2], and the values that row 120 got in each attempt")
    f.text(20, 26, "local[4,2]: the attempt files of the four partitions", size=14, weight=700)
    X0 = 120
    for p in range(4):
        y = 44 + p * 40
        f.text(20, y + 20, f"partition {p}", size=12, mono=True)
        if p != 1:
            f.rect(X0, y + 4, 240, 28, c="ok", fill=0.25, rx=4)
            f.text(X0 + 10, y + 23, f"p{p}_a0", size=12, mono=True)
        else:
            f.rect(X0, y + 4, 240, 28, c="bad", fill=0.25, rx=4)
            f.text(X0 + 10, y + 23, "p1_a0: fails at row 150", size=12, mono=True)
            f.rect(X0 + 260, y + 4, 240, 28, c="ok", fill=0.25, rx=4)
            f.text(X0 + 270, y + 23, "p1_a1: runs all rows again", size=12, mono=True)
            f.line(X0 + 240, y + 18, X0 + 258, y + 18, c="bad", arrow=True)
    f.text(20, 222, "Row 120 is in partition 1, so it ran in both attempts:", size=13, weight=600)
    f.table(20, 230, [140, 200, 200], [("attempt 0", "0.487818", "0.611899"), ("attempt 1", "0.563658", "0.611899")],
            head=["", "random.random()", "rand(7)"], size=12, rowh=22, align=["start", "end", "end"])
    f.text(580, 262, "rand(7): the same value (seed + partition index)", size=12, c="ok")
    f.text(580, 282, "random.random(): a new value on the retry", size=12, c="bad")
    f.text(620, 60, "Under local[4], one failure stops the job.", size=12, c="mute")
    f.text(620, 78, "local[N,F] gives F attempts to a task.", size=12, c="mute")
    return f.save("ch43-attempts", CHECK)


def connect_matrix():
    rows = [("classic", "spark.sql", (1, 1, 1)), ("classic", "spark.sql rand(42)", (1, 1, 1)),
            ("classic", "DataFrame API", (1, 1, 1)), ("connect", "spark.sql", (0, 0, 0)),
            ("connect", "spark.sql rand(42)", (1, 0, 0)), ("connect", "DataFrame API", (1, 1, 0))]
    f = Fig(900, 290, "Same value in two actions on one DataFrame, Classic against Connect")
    f.text(20, 26, "Two actions, 1.1 s apart, on the same DataFrame: is the value the same?", size=14, weight=700)
    cols = ["rand", "uuid", "timestamp"]
    X0, Y0, W, H = 300, 48, 120, 34
    for j, c in enumerate(cols):
        f.text(X0 + j * W + W / 2, Y0 + 14, c, size=12, anchor="middle", c="mute", weight=600)
    for i, (mode, df, vals) in enumerate(rows):
        y = Y0 + 24 + i * H
        f.text(20, y + 22, mode, size=12, mono=True, weight=600, c="si" if mode == "connect" else "ink")
        f.text(100, y + 22, df, size=12, mono=True)
        for j, v in enumerate(vals):
            f.rect(X0 + j * W + 4, y + 3, W - 8, H - 6, c="ok" if v else "bad", fill=0.3, rx=4, stroke=0)
            f.text(X0 + j * W + W / 2, y + 22, "same" if v else "new", size=12, anchor="middle", weight=600)
    f.text(680, 120, "Connect analyzes each action", size=12, c="mute")
    f.text(680, 138, "again. The Python client puts", size=12, c="mute")
    f.text(680, 156, "a seed into F.rand() and", size=12, c="mute")
    f.text(680, 174, "F.uuid(). SQL text gets a new", size=12, c="mute")
    f.text(680, 192, "seed in each analysis.", size=12, c="mute")
    return f.save("ch43-connect-matrix", CHECK)


def fragile():
    data = [("fragile_job", [("charges", 6, 12), ("audit", 1, 6), ("ledger", 12, 24)]),
            ("correct_job", [("charges", 6, 6), ("audit", 2, 2), ("ledger", 6, 6)])]
    f = Fig(900, 280, "Rows in each output of the fragile job and the correct job, clean and under chaos")
    f.text(20, 26, "Rows in each output: a clean run against a chaos run (retries and a rerun)", size=14, weight=700)
    f.rect(600, 14, 14, 14, c="si", fill=0.5, rx=2, stroke=0); f.text(620, 26, "clean", size=12)
    f.rect(680, 14, 14, 14, c="go", fill=0.6, rx=2, stroke=0); f.text(700, 26, "chaos", size=12)
    BASE, S = 220, 6.0
    x = 40
    for job, outs in data:
        f.text(x + 170, 256, job, size=13, mono=True, weight=700, anchor="middle", c="bad" if job.startswith("frag") else "ok")
        for name, clean, chaos in outs:
            f.rect(x, BASE - clean * S, 40, clean * S, c="si", fill=0.5, rx=2, stroke=0)
            f.rect(x + 44, BASE - chaos * S, 40, chaos * S, c="go", fill=0.6, rx=2, stroke=0)
            f.text(x + 20, BASE - clean * S - 5, str(clean), size=11, anchor="middle", mono=True)
            f.text(x + 64, BASE - chaos * S - 5, str(chaos), size=11, anchor="middle", mono=True)
            f.text(x + 42, BASE + 16, name, size=11, anchor="middle", c="mute")
            x += 115
        x += 85
    f.line(30, BASE, 870, BASE, c="line")
    return f.save("ch43-fragile", CHECK)


if __name__ == "__main__":
    for fn in (attempts, connect_matrix, fragile):
        print(fn())
