"""Chapter 92 figures: the build time of each Maven module, and what a hadoop-provided build leaves out.
Reads code/ch92/output/reactor.txt and inspect.txt.

Run from code/:  FIG_CHECK_DIR=<scratch> uv run python ch92/figures.py
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.svgfig import Fig  # noqa: E402

OUT = Path(__file__).resolve().parent / "output"


def secs(s):
    m = re.match(r"(\d+):(\d+) min", s)
    return int(m[1]) * 60 + int(m[2]) if m else float(s.split()[0])


def modules():
    rows = [(m[1].strip(" ."), secs(m[2])) for m in re.finditer(r"^(.*?) \.+ SUCCESS \[\s*(.*?)\]", (OUT / "reactor.txt").read_text(), re.M)]
    total = sum(s for _, s in rows)
    top = sorted(rows, key=lambda r: -r[1])[:12]
    rest = total - sum(s for _, s in top)
    f = Fig(940, 520, "Maven modules by build time")
    f.text(20, 28, f"Build time of each Maven module, v4.3.0, JDK 17 in Docker (reactor.txt, {len(rows)} modules)", 17, 700, anchor="start")
    x0, w = 300, 560
    mx = top[0][1]
    for i, (name, s) in enumerate(top + [(f"the other {len(rows) - len(top)} modules", rest)]):
        y = 50 + i * 34
        f.text(x0 - 10, y + 20, name, 12.5, 500, anchor="end")
        kind = "acc" if i < 3 else ("ghost" if i == len(top) else "blue")
        f.rect(x0, y + 6, max(2, w * s / max(mx, rest)), 22, kind, r=3)
        f.text(x0 + w * s / max(mx, rest) + 8, y + 22, f"{s:.0f} s", 12, 600, anchor="start", c="mute")
    f.text(20, 505, f"Sum of the module times: {total / 60:.1f} min. Maven reported \"Total time: 26:43 min\" for the run.",
           12.5, 400, anchor="start", c="mute")
    f.save("ch92-module-times")


def jars():
    t = (OUT / "inspect.txt").read_text()
    off = int(re.search(r"jars/\s+(\d+) jars", t)[1])
    ours = int(re.search(r"jars/\s+\d+ jars\s+(\d+) jars", t)[1])
    missing = int(re.search(r"not in this build: (\d+)", t)[1])
    hive = int(re.search(r"Hive and its dependencies: (\d+) jars", t)[1])
    spark_mods = len(re.findall(r"^    spark-", t, re.M))
    other = missing - hive - spark_mods
    f = Fig(940, 300, "Jars: official tarball and this build")
    f.text(20, 28, "The jars/ folder: the official -bin-hadoop3 tarball and this build (inspect.txt)", 17, 700, anchor="start")
    x0, W = 200, 700
    sc = W / off
    f.text(190, 82, f"official, {off} jars", 13, 600, anchor="end")
    segs = [(ours, "in both", "blue"), (hive, "Hive + deps", "purple"), (other, "Hadoop client, slf4j, ...", "acc"),
            (spark_mods, "", "bad")]
    x = x0
    for n, lab, k in segs:
        f.rect(x, 60, n * sc, 34, k, r=2)
        if lab:
            f.text(x + n * sc / 2, 82, f"{lab} ({n})" if n > 20 else f"{n}", 12, 600, c=k)
        x += n * sc
    f.text(190, 152, f"this build, {ours} jars", 13, 600, anchor="end")
    f.rect(x0, 130, ours * sc, 34, "blue", r=2)
    f.text(x0 + ours * sc / 2, 152, f"in both ({ours})", 12, 600, c="blue")
    f.text(x0, 200, f"Missing from this build: {hive} Hive jars and their libraries and {spark_mods} Spark modules (no -Phive,"
           " -Phive-thriftserver, -Pyarn),", 12.5, 400, anchor="start", c="mute")
    f.text(x0, 218, f"and {other} other jars: the Hadoop client, ZooKeeper, Curator, Guava, slf4j-api and more"
           " (-Phadoop-provided).", 12.5, 400, anchor="start", c="mute")
    f.text(x0, 250, "SPARK_DIST_CLASSPATH puts them back at run time. Without it, the build fails with"
           " NoClassDefFoundError.", 12.5, 600, anchor="start")
    f.save("ch92-jars")


modules()
jars()
