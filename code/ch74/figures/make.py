"""Chapter 74 figures. Numbers from code/ch74/output/eventlog.txt, profilers.txt and connect_obs.txt."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def diagnosis():
    st = [(0, "skewed_scoring", 450, 30000, 30000, 1.0, 0, 0, []),
          (1, "skewed_scoring", 9227, 55364, 1638, 33.8, 0, 6840, ["skew", "python"]),
          (2, "skewed_scoring", 124, 27, 24, 1.1, 0, 0, []),
          (3, "spilling_sort", 145, 15000, 15000, 1.0, 0, 0, []),
          (4, "spilling_sort", 571, 15163, 15000, 1.0, 367016, 0, ["spill"])]
    f = Fig(900, 300, "The diagnosis of each stage from the event log")
    f.text(20, 26, "Event log diagnosis: run time of each stage, with the Python share and the flags", size=14, weight=700)
    X0, S = 220, 0.035
    for i, (sid, desc, run, mx, med, skew, spill, py, flags) in enumerate(st):
        y = 46 + i * 44
        f.text(20, y + 16, f"stage {sid}", size=12, mono=True, weight=600)
        f.text(20, y + 32, desc, size=11, c="mute")
        f.rect(X0, y + 6, run * S, 24, c="si", fill=0.3, rx=3, stroke=0)
        if py:
            f.rect(X0, y + 6, py * S, 24, c="go", fill=0.55, rx=3, stroke=0)
        f.text(X0 + run * S + 8, y + 23, f"{run:,} ms", size=12, mono=True)
        x = X0 + run * S + 90
        for fl in flags:
            c = {"skew": "bad", "python": "go", "spill": "pu"}[fl]
            f.rect(x, y + 8, 64, 20, c=c, fill=0.25, rx=10)
            f.text(x + 32, y + 22, fl, size=11, anchor="middle", weight=600)
            x += 70
        if spill:
            f.text(x + 6, y + 23, f"{spill:,} bytes spilled", size=11, c="pu")
    f.rect(X0, 272, 14, 14, c="go", fill=0.55, rx=2, stroke=0); f.text(X0 + 20, 284, "time in Python (6,840 ms)", size=11)
    f.rect(X0 + 200, 272, 14, 14, c="si", fill=0.3, rx=2, stroke=0); f.text(X0 + 220, 284, "run time of all tasks", size=11)
    f.text(X0 + 400, 284, "stage 1 skew: max 55,364 rows, median 1,638 (33.8x)", size=11, c="bad")
    return f.save("ch74-diagnosis", CHECK)


def profiler():
    rows = [("cheap", 0.28, 5.94, 21.4), ("slow", 1.49, 6.20, 4.2), ("pandas", 0.16, 0.13, 0.8)]
    f = Fig(900, 240, "Wall time of three UDFs with the perf profiler off and on")
    f.text(20, 26, "200,000 rows: median wall time of 5 runs, profiler off against spark.sql.pyspark.udf.profiler=perf",
           size=14, weight=700)
    X0, S = 120, 100
    for i, (u, off, on, r) in enumerate(rows):
        y = 50 + i * 58
        f.text(20, y + 26, u, size=13, mono=True, weight=600)
        f.rect(X0, y + 4, off * S, 20, c="si", fill=0.5, rx=2, stroke=0)
        f.text(X0 + off * S + 6, y + 19, f"{off:.2f}s off", size=11, mono=True)
        f.rect(X0, y + 28, on * S, 20, c="go", fill=0.6, rx=2, stroke=0)
        f.text(X0 + on * S + 6, y + 43, f"{on:.2f}s perf, ratio {r}", size=11, mono=True, weight=600,
               c="bad" if r > 2 else "ink")
    f.text(20, 230, "The row profiler wraps each call. One pandas call covers a full Arrow batch.", size=12, c="mute")
    return f.save("ch74-profiler", CHECK)


def connect_ops():
    f = Fig(900, 300, "Two Connect sessions, their operations and the jobs of each operation")
    f.text(20, 26, "/connect/sessions and /connect/operations of the live server", size=14, weight=700)
    f.box(20, 60, 200, 60, "session b5c2f72d", "user analyst", c="si", size=13)
    f.box(20, 170, 200, 60, "session 04690712", "user agent", c="go", size=13)
    ops = [(60, "operation 4bcca073", "CLOSED, 2349 ms, tag daily-report", ["0", "1", "2"], "si", 90),
           (150, "operation 63a9c4dd", "error: UNRESOLVED_COLUMN, 201 ms", ["3"], "bad", 200),
           (220, "operation 3fbe2e6d", "CLOSED, 159 ms", ["4", "5"], "go", 200)]
    for y, name, sub, jobs, c, sy in ops:
        f.line(220, sy, 296, y + 24, c="line", arrow=True)
        f.box(300, y, 300, 48, name, sub, c=c, size=13, mono=False)
        for k, j in enumerate(jobs):
            f.line(600, y + 24, 640 + k * 70, y + 24, c="line") if k == 0 else None
            f.rect(644 + k * 70, y + 8, 60, 32, c="gr", fill=0.15, rx=5)
            f.text(674 + k * 70, y + 29, f"job {j}", size=12, anchor="middle", mono=True)
    f.text(20, 290, "The failed operation still ran job 3: it read the Parquet footers before the analyzer found the error.",
           size=12, c="mute")
    return f.save("ch74-connect-ops", CHECK)


if __name__ == "__main__":
    for fn in (diagnosis, profiler, connect_ops):
        print(fn())
