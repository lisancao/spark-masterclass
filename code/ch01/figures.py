"""Chapter 1 figures: MapReduce against RDD data flow, and the measured pass times.

Reads code/ch01/output/iterative.txt. Run from code/:  uv run python ch01/figures.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.svgfig_bg import write, text, box, arrow, cylinder, rect

HERE = Path(__file__).resolve().parent


def dataflow(c):
    out = []
    out.append(text(20, 30, "MapReduce: each pass is a separate job", c, 17, c["acc"], 700, "start"))
    y = 60
    for i, label in enumerate(("pass 1", "pass 2", "pass 3")):
        x0 = 20 + i * 300
        out.append(cylinder(x0, y, 70, 60, c, "HDFS"))
        out.append(box(x0 + 95, y + 2, 60, 26, c, "map", fill=c["acc_soft"], stroke=c["acc"], size=12))
        out.append(cylinder(x0 + 95, y + 34, 60, 34, c, "disk", size=11))
        out.append(box(x0 + 180, y + 2, 70, 26, c, "reduce", fill=c["acc_soft"], stroke=c["acc"], size=12))
        out.append(arrow(x0 + 70, y + 15, x0 + 93, y + 15, c))
        out.append(arrow(x0 + 155, y + 15, x0 + 178, y + 15, c))
        out.append(arrow(x0 + 125, y + 28, x0 + 125, y + 40, c, sw=1.2))
        out.append(arrow(x0 + 250, y + 15, x0 + 292, y + 15, c))
        out.append(text(x0 + 135, y + 92, label, c, 12, c["mute"]))
    out.append(cylinder(920, y, 60, 60, c, "HDFS"))
    out.append(text(500, y + 118, "Input is read and parsed again in every pass. "
                    "State goes through files between jobs.", c, 13, c["mute"]))
    y2 = 220
    out.append(text(20, y2, "Spark RDD: read once, keep the working set in memory", c, 17, c["blue"], 700, "start"))
    out.append(cylinder(20, y2 + 30, 70, 60, c, "files"))
    out.append(box(115, y2 + 35, 90, 50, c, "parse", "once", fill=c["blue_soft"], stroke=c["blue"], size=13))
    out.append(arrow(90, y2 + 60, 113, y2 + 60, c))
    out.append(rect(240, y2 + 22, 160, 76, c, fill=c["blue_soft"], stroke=c["blue"], r=10))
    out.append(text(320, y2 + 50, "cache()", c, 14, c["blue"], 700, mono=True))
    out.append(text(320, y2 + 72, "partitions in", c, 12, c["mute"]))
    out.append(text(320, y2 + 88, "executor memory", c, 12, c["mute"]))
    out.append(arrow(205, y2 + 60, 238, y2 + 60, c))
    for i in range(4):
        x = 450 + i * 130
        out.append(box(x, y2 + 40, 100, 40, c, f"pass {i + 1}" if i < 3 else "pass n",
                       fill=c["panel"], stroke=c["blue"], size=13))
        out.append(arrow(400, y2 + 60, x, y2 + 60, c, color=c["blue"], sw=1.2, head=(i == 0)) if i == 0 else [])
    for i in range(3):
        x = 450 + i * 130
        out.append(arrow(x + 100, y2 + 60, x + 128, y2 + 60, c, color=c["mute"], sw=1.2))
    out.append('<path d="M320,%d C320,%d 880,%d 880,%d" fill="none" stroke="%s" stroke-width="1.4" stroke-dasharray="4 4"/>'
               % (y2 + 98, y2 + 135, y2 + 135, y2 + 82, c["blue"]))
    out.append(text(600, y2 + 140, "every pass reads the cached partitions; only the small result returns to the driver",
                    c, 13, c["mute"]))
    return out


def pass_times(c):
    rows = []
    for line in (HERE / "output" / "iterative.txt").read_text().splitlines():
        m = re.match(r"\s+(\d+)\s+([\d.]+)\s+[\d,]+\s+([\d.]+)\s+[\d,]+$", line)
        if m:
            rows.append((int(m[1]), float(m[2]), float(m[3])))
    W, H, L, B, T = 760, 330, 60, 270, 40
    top = max(max(r[1], r[2]) for r in rows)
    scale = (B - T) / (top * 1.1)
    out = [text(20, 24, "Seconds per pass, same job, same answer (iterative.txt)", c, 15, c["fg"], 700, "start")]
    for v in (0.5, 1.0, 1.5, 2.0, 2.5):
        if v <= top * 1.1:
            y = B - v * scale
            out.append(f'<line x1="{L}" y1="{y:.1f}" x2="{W - 20}" y2="{y:.1f}" stroke="{c["line"]}" stroke-width="0.6" stroke-dasharray="3 3"/>')
            out.append(text(L - 8, y + 4, f"{v:.1f}", c, 11, c["mute"], anchor="end"))
    gw = (W - L - 40) / len(rows)
    for p, mr, rd in rows:
        x = L + 10 + (p - 1) * gw
        for k, (v, col) in enumerate(((mr, c["acc"]), (rd, c["blue"]))):
            h = v * scale
            out.append(f'<rect x="{x + k * 26:.1f}" y="{B - h:.1f}" width="24" height="{h:.1f}" fill="{col}" rx="2"/>')
        out.append(text(x + 25, B + 18, str(p), c, 12, c["mute"]))
    out.append(f'<line x1="{L}" y1="{B}" x2="{W - 20}" y2="{B}" stroke="{c["line"]}" stroke-width="1"/>')
    out.append(text((L + W) / 2, B + 40, "pass", c, 12, c["mute"]))
    out.append(f'<rect x="{W - 300}" y="40" width="14" height="14" fill="{c["acc"]}" rx="2"/>')
    out.append(text(W - 280, 52, "MapReduce style: read text, parse, shuffle, write", c, 12, c["fg"], anchor="start"))
    out.append(f'<rect x="{W - 300}" y="62" width="14" height="14" fill="{c["blue"]}" rx="2"/>')
    out.append(text(W - 280, 74, "RDD with cache(): pass 1 fills the cache", c, 12, c["fg"], anchor="start"))
    return out


write("ch01-dataflow", 1000, 380, dataflow)
write("ch01-pass-times", 760, 330, pass_times)
print("ok")
