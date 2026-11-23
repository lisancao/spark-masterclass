"""Figures for Chapter 86. Run: python3 ch86/figures.py (from code/). Numbers are parsed from output/*.txt."""
import math
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig

OUT = Path(__file__).resolve().parent / "output"


def estimates():
    txt = (OUT / "stats.txt").read_text()
    rows = re.findall(r"^  (total_amount[^\d]*?[\d ANDBETWEEN]*?|payment_type = \d)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)$", txt, re.M)
    rows = [(p.strip(), *(int(v.replace(",", "")) for v in vals)) for p, *vals in rows]
    W, H = 900, 70 + 52 * len(rows) + 70
    f = Fig(W, H, "Estimated and real rows for six filters")
    f.text(20, 22, "Rows that each filter keeps: real, estimated without a histogram, estimated with a histogram (stats.txt)",
           size=13, anchor="start", color="mute")
    x0, x1 = 270, 870
    lo, hi = 1, 1e7
    sx = lambda v: x0 + (math.log10(max(v, lo)) - 0) / (math.log10(hi)) * (x1 - x0)
    ty = 52
    for d in range(0, 8):
        x = sx(10 ** d)
        f.line(x, ty, x, H - 70, color="line", dash="2 4")
        f.text(x, ty - 6, f"{10 ** d:,}" if d < 7 else "10,000,000", size=10.5, color="mute")
    for i, (p, real, uni, hist) in enumerate(rows):
        y = 80 + i * 52
        f.text(x0 - 12, y, p, size=12.5, anchor="end", mono=True)
        f.line(sx(min(real, uni, hist)), y, sx(max(real, uni, hist)), y, color="line", sw=2)
        f.circle(sx(uni), y, 7, fill="red")
        f.circle(sx(hist), y, 7, fill="blue")
        f.circle(sx(real), y, 4.5, fill="fg")
    ly = H - 40
    x = 270
    for k, t in [("fg", "real rows"), ("red", "estimate, min and max only"), ("blue", "estimate, with a histogram")]:
        f.circle(x, ly, 6, fill=k); f.text(x + 12, ly, t, size=12.5, anchor="start"); x += 220
    f.save("ch86-estimates")


def dpp():
    txt = (OUT / "dpp.txt").read_text()
    m = {lab.strip(): (int(p), int(r.replace(",", "")), float(w), float(t)) for lab, p, r, w, t in
         re.findall(r"^  (DPP on \(default\)|DPP off|DPP on, broadcast off)\s+partitions read\s+(\d+)\s+rows out of scan\s+([\d,]+)"
                    r"\s+task time\s+([\d.]+) s\s+wall ([\d.]+) s", txt, re.M)}
    W, H = 900, 300
    f = Fig(W, H, "Partitions and rows that the fact scan read")
    f.text(20, 22, "Sundays only: what the scan of trips_by_day read (dpp.txt; 181 partitions, 22,131,665 rows)",
           size=13, anchor="start", color="mute")
    x0, x1 = 230, 860
    for i, (lab, kind) in enumerate([("DPP on (default)", "ok"), ("DPP off", "red"), ("DPP on, broadcast off", "red")]):
        p, r, work, wall = m[lab]
        y = 60 + i * 70
        f.text(x0 - 12, y + 22, lab, size=13, anchor="end")
        w = (x1 - x0) * p / 181
        f.box(x0, y, w, 44, "", "", kind=kind, rx=4)
        lx = x0 + w + 12 if w < 420 else x0 + 14
        f.text(lx, y + 22, f"{p} of 181 partitions   {r:,} rows   task time {work:.1f} s   wall {wall:.2f} s", size=13,
               anchor="start", weight=600 if kind == "ok" else 400)
    f.note("bar length = partitions read; wall time: median of 7 runs after a warmup, under the bench lock", y=H - 18)
    f.save("ch86-dpp")


def joins():
    b = (OUT / "bucketing.txt").read_text()
    sp = (OUT / "spj.txt").read_text()
    rows = []
    for lab, ex, t in re.findall(r"^  (\S.*?)\s{2,}SortMergeJoin\s+exchanges (\d)\s+([\d.]+) s", b, re.M):
        rows.append(("bucketed table: " + re.sub(r"\s+", " ", lab), int(ex), float(t)))
    for lab, ex, t in re.findall(r"^  (\S.*?)\s{2,}SortMergeJoin\s+Exchange nodes (\d)\s+([\d.]+) s", sp, re.M):
        rows.append(("Iceberg: " + lab, int(ex), float(t)))
    W, H = 920, 60 + len(rows) * 36 + 40
    f = Fig(W, H, "Exchange nodes before the join, for each pair of tables")
    f.text(20, 22, "The same join with broadcast off: Exchange nodes before the join (bucketing.txt, spj.txt)",
           size=13, anchor="start", color="mute")
    x0 = 420
    for i, (lab, ex, t) in enumerate(rows):
        y = 50 + i * 36
        f.text(x0 - 12, y + 13, lab, size=12.5, anchor="end")
        if ex == 0:
            f.box(x0, y, 200, 26, "no shuffle", "", kind="ok", size=12, rx=4)
        for k in range(ex):
            f.box(x0 + k * 104, y, 96, 26, "Exchange", "", kind="red", size=12, rx=4, weight=500)
        f.text(x0 + 330, y + 13, f"{t:.2f} s", size=12, anchor="start", color="mute", mono=True)
    f.note("times: median of 5 runs after a warmup, under the bench lock, on a shared machine", y=H - 14)
    f.save("ch86-joins")


if __name__ == "__main__":
    joins()
    estimates()
    dpp()
