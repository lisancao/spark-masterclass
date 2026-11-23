"""Figures for Chapter 27. Versions from the build files; results from code/ch27/output/."""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "output"
CLIENTS = ["python", "scala", "go", "rust", "swift"]

# 1. The client landscape: one server, five clients, each with its three libraries
cards = [
    ("Python", "pyspark-client 4.3.0", "PyPI", "grpcio", "protobuf", "pyarrow", "apache/spark"),
    ("Scala / Java", "spark-connect-client-jvm", "Maven Central, _2.13:4.3.0", "grpc-java 1.76.0", "protobuf-java 4.33.5",
     "Arrow Java 19.0.0", "apache/spark"),
    ("Go", "spark-connect-go v0.1.0", "Go module .../v40", "grpc-go 1.72.2", "protobuf-go 1.36.6", "arrow-go 18.3.0",
     "apache/spark-connect-go"),
    ("Rust", "apache-spark-connect 4.3.0", "crates.io", "tonic 0.14.9", "prost 0.14.6", "arrow-rs 59.3.3",
     "apache/spark-connect-rust"),
    ("Swift", "spark-connect-swift 0.8.0", "Swift Package Manager", "grpc-swift-2 2.4.4", "swift-protobuf 1.41.0",
     "its own Arrow reader", "apache/spark-connect-swift"),
]
f = Fig(1000, 470, "Five Spark Connect clients and one server")
f.text(20, 26, "Five clients, one protocol: each client is a gRPC stub, protobuf messages and an Arrow reader", 15, 700,
       anchor="start")
cw, gap, y0 = 184, 10, 48
for i, (lang, pkg, where, g, p, a, repo) in enumerate(cards):
    x = 20 + i * (cw + gap)
    kind = "blue" if repo == "apache/spark" else "purple"
    f.rect(x, y0, cw, 236, kind, r=10)
    f.text(x + cw / 2, y0 + 26, lang, 16, 700, c=kind)
    f.text(x + cw / 2, y0 + 48, pkg, 11, 600, c=kind, mono=True)
    f.text(x + cw / 2, y0 + 64, where, 11, 400, c="mute")
    for j, (lab, val) in enumerate([("gRPC", g), ("protobuf", p), ("Arrow", a)]):
        yy = y0 + 82 + j * 44
        f.rect(x + 10, yy, cw - 20, 36, "n", r=6)
        f.text(x + 18, yy + 14, lab, 10, 700, anchor="start", c="mute")
        f.text(x + 18, yy + 29, val, 11, 500, anchor="start", mono=True)
    f.text(x + cw / 2, y0 + 226, repo, 10.5, 500, c="mute", mono=True)
    cx = x + cw / 2
    f.arrow(cx - 22, y0 + 240, cx - 22, 360, "", c="acc")
    f.arrow(cx + 22, 360, cx + 22, y0 + 240, "", c="ok")
f.text(84, 322, "plan", 11, 600, anchor="end", c="acc")
f.text(84, 336, "(protobuf)", 11, 600, anchor="end", c="acc")
f.text(140, 322, "Arrow", 11, 600, anchor="start", c="ok")
f.text(140, 336, "batches", 11, 600, anchor="start", c="ok")
f.box(20, 362, 960, 62, "Spark 4.3.0 Connect server: sc://localhost:15106",
      "the same server, the same plan, the same five rows from each client", kind="acc", size=16, subsize=12)
f.rect(20, 438, 14, 14, "blue", r=3)
f.text(40, 450, "in the apache/spark repository, released with Spark", 11, 400, anchor="start", c="mute")
f.rect(400, 438, 14, 14, "purple", r=3)
f.text(420, 450, "a separate Apache Spark repository, its own release", 11, 400, anchor="start", c="mute")
f.save("ch27-landscape")

# 2. Capability grid, from the probe outputs and the reattach check
cases = ["sql", "schema", "local_data", "temp_view", "write_read", "config", "error", "arrow", "explain", "zip",
         "big_result"]
grid = {}
for c in CLIENTS:
    for line in (OUT / f"{c}-probe-default.txt").read_text().splitlines():
        m = re.match(r"(\w+)\s+(works|breaks|n/o)\s+(.*)", line)
        if m:
            grid[(c, m.group(1))] = m.group(2)
re_rows = {}
for line in (OUT / "reattach-small-streams.txt").read_text().splitlines()[1:]:
    parts = line.split()
    re_rows[parts[0]] = int(parts[-1])
labels = {"local_data": "local data", "temp_view": "temp view", "write_read": "write + read", "big_result": "20 MB result"}
f = Fig(940, 300, "What each client did in the probe")
f.text(20, 24, "The same probe in each client, against the Spark 4.3.0 server (measured for this chapter)", 14, 700,
       anchor="start")
x0, y0, cwid, rh = 120, 60, 68, 36
for j, case in enumerate(cases + ["reattach"]):
    f.text(x0 + j * cwid + cwid / 2, y0 - 10, labels.get(case, case), 11, 600)
for i, c in enumerate(CLIENTS):
    y = y0 + i * rh
    f.text(x0 - 12, y + 23, c.capitalize(), 13, 700, anchor="end")
    for j, case in enumerate(cases + ["reattach"]):
        if case == "reattach":
            n = re_rows[c]
            res, txt = ("works", f"{n} calls") if n else ("n/o", "not asked")
        else:
            res = grid[(c, case)]
            txt = {"works": "works", "n/o": "no API", "breaks": "breaks"}[res]
        kind = {"works": "ok", "n/o": "ghost", "breaks": "bad"}[res]
        f.rect(x0 + j * cwid + 3, y + 4, cwid - 6, rh - 8, kind, r=5, dash=(res == "n/o"))
        f.text(x0 + j * cwid + cwid / 2, y + 23, txt, 10.5, 600, c=kind)
f.text(20, 272, "reattach: ReattachExecute calls for the 20 MB result when the server ends each stream after 1 MiB.", 11,
       400, anchor="start", c="mute")
f.text(20, 289, "Go v0.1.0 sends its executions as not reattachable, so it never needs to reattach.", 11, 400,
       anchor="start", c="mute")
f.save("ch27-probe")

# 3. Time from main() to the five rows (median of 5, bench lock) and the size of each program
times = {}
for line in (OUT / "taxi-timings.txt").read_text().splitlines()[1:]:
    p = line.split()
    times[p[0]] = float(p[1])
sizes = {}
for line in (OUT / "build-facts.txt").read_text().splitlines()[1:]:
    p = line.split()
    sizes[p[0]] = (float(p[1]), int(p[2]))
f = Fig(900, 275, "Time to the result and size of each client program")
f.text(20, 24, "Connect, send the plan, show 5 rows: median of 5 runs (s)", 14, 700, anchor="start")
f.text(480, 24, "Compiled program size (MB), one release build", 14, 700, anchor="start")
for i, c in enumerate(CLIENTS):
    y = 46 + i * 40
    f.text(90, y + 19, c.capitalize(), 13, 700, anchor="end")
    w = times[c] / 2.0 * 330
    f.rect(100, y + 4, w, 24, "acc" if c in ("python", "scala") else "blue", r=4)
    f.text(106 + w, y + 21, f"{times[c]:.2f} s", 12, 600, anchor="start")
    f.text(560, y + 19, c.capitalize(), 13, 700, anchor="end")
    if c in sizes:
        bw = sizes[c][1] / 110 * 300
        f.rect(570, y + 4, bw, 24, "purple", r=4)
        f.text(576 + bw, y + 21, f"{sizes[c][1]} MB", 12, 600, anchor="start")
    else:
        f.text(570, y + 21, "JAR files on a JVM classpath" if c == "scala" else "a Python package for an interpreter", 12, 400,
               anchor="start", c="mute", italic=True)
f.text(20, 265, "Times include the connection and the server work. Python and Scala start their runtime before main().", 11,
       400, anchor="start", c="mute")
f.save("ch27-measured")
