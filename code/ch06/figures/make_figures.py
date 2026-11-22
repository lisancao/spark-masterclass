"""Figures for Chapter 6. Numbers from the chapter text and code/ch06/output/check_env.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. Size on disk of each install (chapter table "On disk").
f = Fig(820, 250, "Size on disk of each Spark 4.3 install")
rows = [("Docker image apache/spark:4.3.0-python3", 1330, "1.33 GB", "blue"),
        ("Tarball, unpacked", 599, "599 MB", "purple"),
        ("pip install pyspark", 484, "484 MB (280 jars)", "acc"),
        ("pip install pyspark-client", 11, "11 MB (+ pandas, pyarrow, grpcio)", "ok")]
x0, maxw = 300, 330
f.text(20, 26, "Size on disk", 15, 700, anchor="start")
for i, (lab, mb, txt, k) in enumerate(rows):
    y = 50 + i * 46
    f.text(x0 - 12, y + 20, lab, 13, 500, anchor="end")
    w = max(3, maxw * mb / 1330)
    f.rect(x0, y + 4, w, 24, k, r=4)
    f.text(x0 + w + 8, y + 21, txt, 13, 600, anchor="start", c=k)
f.line(x0, 46, x0, 236, c="grid")
f.save("ch06-install-sizes")

# 2. What each install puts on the machine, layer by layer.
f = Fig(860, 400, "What each install puts on your machine")
cols = [
    ("1. pip", [("your Python 3.11+", "blue"), ("pyspark package\n280 jars inside", "acc"), ("driver JVM\nchild process, Py4J", "acc"), ("your JDK 17, 21 or 25", "n")]),
    ("2. Tarball", [("your Python (optional)", "blue"), ("spark-4.3.0-bin-hadoop3/\nbin  sbin  conf  jars", "acc"), ("driver JVM\nfrom spark-submit", "acc"), ("your JDK 17, 21 or 25", "n")]),
    ("3. Docker", [("container", None), ("Python 3.10.12", "purple"), ("/opt/spark + driver JVM", "purple"), ("Java 21 (in the image)", "purple")]),
    ("4. Spark Connect", [("your Python 3.11+", "blue"), ("pyspark-client\n11 MB, no jars", "ok"), ("no JVM", None), ("no JDK", None)]),
]
cw, gap, x = 196, 14, 18
for title, layers in cols:
    f.text(x + cw / 2, 30, title, 15, 700)
    f.rect(x, 42, cw, 300, "ghost", r=10, dash=True)
    f.text(x + cw / 2, 60, "your machine", 11, 500, c="mute")
    y = 72
    if title.startswith("3"):
        f.rect(x + 8, y, cw - 16, 258, "purple", r=8, fill="none")
        f.text(x + cw / 2, y + 18, "container", 12, 600, c="purple")
        y += 30
        layers = layers[1:]
        for lab, k in layers:
            f.box(x + 16, y, cw - 32, 62, lab, kind=k, size=12)
            y += 72
    else:
        for lab, k in layers:
            if k is None:
                f.box(x + 8, y, cw - 16, 54, lab, kind="ghost", dash=True, size=12, weight=500)
            else:
                t, *s = lab.split("\n")
                f.box(x + 8, y, cw - 16, 54, t, "\n".join(s), kind=k, size=12, subsize=11)
            y += 64
    x += cw + gap
# connect server arrow
f.text(x - gap - cw / 2, 362, "gRPC, port 15002, to a", 11, 600, c="ok")
f.text(x - gap - cw / 2, 378, "Connect server with the driver JVM", 11, 600, c="ok")
f.save("ch06-what-runs-where")

# 3. The data problems in the first summary (check_env.txt).
f = Fig(820, 190, "Problems in the June 2026 taxi data")
f.text(20, 26, "describe() on 3,837,248 trips", 15, 700, anchor="start")
# panel a: passenger_count nulls
px = 20
f.text(px, 58, "passenger_count", 13, 700, anchor="start", mono=True)
present = 2823748 / 3837248
f.rect(px, 70, 240 * present, 30, "blue", r=3)
f.rect(px + 240 * present, 70, 240 * (1 - present), 30, "bad", r=3)
f.text(px, 120, "2,823,748 with a value", 12, 500, anchor="start", c="blue")
f.text(px + 240, 136, "1,013,500 null (26%)", 12, 600, anchor="end", c="bad")
# panel b: trip_distance
px = 300
f.text(px, 58, "trip_distance (miles)", 13, 700, anchor="start", mono=True)
f.line(px, 86, px + 230, 86, c="line")
for v, lab, k, xo in ((0, "min 0.0", "n", 0), (5.2, "mean 5.20", "blue", 6), (311565.39, "max 311,565.39", "bad", 230)):
    f.circle(px + xo, 86, 6, kind=k)
f.text(px, 112, "min 0.0", 12, 500, anchor="start", c="mute")
f.text(px + 14, 104 + 26, "mean 5.20", 12, 600, anchor="start", c="blue")
f.text(px + 230, 112, "max 311,565.39", 12, 700, anchor="end", c="bad")
f.text(px + 230, 130, "stddev 532.85", 12, 400, anchor="end", c="mute")
# panel c: fare_amount
px = 570
f.text(px, 58, "fare_amount ($)", 13, 700, anchor="start", mono=True)
f.line(px, 86, px + 230, 86, c="line")
zero = px + 230 * 1683.7 / (1683.7 + 7045.0)
f.line(zero, 79, zero, 93, c="fg")
f.text(zero, 75, "0", 11, 600)
f.circle(px, 86, 6, kind="bad")
f.circle(zero + 230 * 21.44 / 8728.7, 86, 5, kind="blue")
f.circle(px + 230, 86, 6, kind="acc")
f.text(px, 112, "min -1,683.70", 12, 700, anchor="start", c="bad")
f.text(px + 230, 112, "max 7,045.00", 12, 600, anchor="end", c="acc")
f.text(px, 130, "mean 21.44", 12, 500, anchor="start", c="blue")
f.text(20, 176, "Each value comes from the describe() output of check_env.py. The scales are not linear.", 12, 400, anchor="start", c="mute", italic=True)
f.save("ch06-data-problems")
