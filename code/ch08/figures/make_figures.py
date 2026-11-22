"""Figures for Chapter 8. Numbers from code/ch08/output/layers.txt, static_runtime.txt and
connect_conf.txt. Run: FIG_CHECK_DIR=<scratch> python3 code/ch08/figures/make_figures.py"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. The layers: each row adds one source (layers.txt, section 1).
rows = [  # label, shuffle, driver.memory in SparkConf, JVM heap MiB, winner kind
    ("built-in defaults only", "200", "(not set)", "1024", "n"),
    ("+ spark-env.sh  SPARK_DRIVER_MEMORY=3g", "200", "3g", "3072", "n"),
    ("+ spark-defaults.conf  11, 1g", "11", "1g", "1024", "n"),
    ("+ --conf  12, 2g", "12", "2g", "2048", "n"),
    ("+ --driver-memory 1500m", "12", "1500m", "1500", "n"),
    ("+ builder.config  13, 4g", "13", "4g", "1500", "bad"),
]
f = Fig(900, 390, "One key in more places each time")
f.text(20, 28, "Each row is a new session with one more source. What wins?", 15, 700, anchor="start")
X = [20, 400, 560, 720]
f.text(X[1] + 70, 62, "spark.sql.shuffle.partitions", 12, 700, c="mute")
f.text(X[2] + 70, 62, "spark.driver.memory", 12, 700, c="mute")
f.text(X[2] + 70, 77, "(the SparkConf says)", 11, 400, c="mute")
f.text(X[3] + 80, 62, "JVM heap", 12, 700, c="mute")
f.text(X[3] + 80, 77, "(what really happened)", 11, 400, c="mute")
for i, (lab, sh, dm, heap, k) in enumerate(rows):
    y = 90 + i * 44
    f.rect(X[0], y, 370, 36, "blue" if i else "n", r=6)
    f.text(X[0] + 12, y + 23, lab, 13, 500, anchor="start", c="blue" if i else "fg", mono=i > 0)
    changed_sh = i == 0 or sh != rows[i - 1][1]
    f.box(X[1] + 20, y, 100, 36, sh, kind="acc" if changed_sh else "ghost", size=15, mono=True)
    changed_dm = i == 0 or dm != rows[i - 1][2]
    f.box(X[2] + 20, y, 100, 36, dm, kind="acc" if changed_dm else "ghost", size=14, mono=True)
    hk = "bad" if k == "bad" else ("acc" if i == 0 or heap != rows[i - 1][3] else "ghost")
    f.box(X[3] + 20, y, 120, 36, f"{heap} MiB", kind=hk, size=14, mono=True)
f.text(X[3] + 80, 370, "4g in the SparkConf, but the JVM", 11.5, 600, c="bad")
f.text(X[3] + 80, 384, "had already started with 1500m", 11.5, 600, c="bad")
f.text(20, 372, "orange = this row changed the value", 11, 400, anchor="start", c="mute", italic=True)
f.save("ch08-layers")

# 2. Who may change what, and when (static_runtime.txt, connect_conf.txt).
cols = ["runtime SQL\nspark.sql.shuffle.partitions", "static SQL\nspark.sql.warehouse.dir",
        "core\nspark.executor.memory"]
acts = [
    ("before the driver starts\n(conf file, --conf, builder)", ["set", "set", "set"]),
    ("builder on a session\nthat already exists", ["set", "ignored", "ignored"]),
    ("spark.conf.set or SET\n(Classic)", ["set", "CANNOT_MODIFY_\nSTATIC_CONFIG", "CANNOT_MODIFY_\nCONFIG"]),
    ("Connect client builder", ["set", "ignored", "ignored"]),
    ("Connect spark.conf.set", ["set", "CANNOT_MODIFY_\nSTATIC_CONFIG", "CANNOT_MODIFY_\nCONFIG"]),
]
f = Fig(900, 444, "Who may change what, and when")
f.text(20, 28, "Who may change a config, and when", 15, 700, anchor="start")
cx, cw, rh, top = 250, 210, 62, 50
for j, c in enumerate(cols):
    f.box(cx + j * cw + 5, top, cw - 10, 50, c.split("\n")[0], c.split("\n")[1], kind="n", size=14,
          sub_mono=True, subsize=11)
for i, (lab, cells) in enumerate(acts):
    y = top + 60 + i * rh
    f.text(20, y + 26, lab, 12.5, 600, anchor="start")
    for j, v in enumerate(cells):
        k = {"set": "ok", "ignored": "ghost"}.get(v, "bad")
        f.box(cx + j * cw + 5, y + 4, cw - 10, rh - 10, v, kind=k, size=12.5 if k == "bad" else 14,
              mono=(k == "bad"))
f.text(20, 436, "ignored = no error, and the value does not change", 11, 400, anchor="start", c="mute",
       italic=True)
f.save("ch08-who-may-change")
