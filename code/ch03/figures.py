"""Chapter 3 figures: the measured times, and the bytes of one UnsafeRow.

Reads code/ch03/output/*-run*.txt and tungsten.txt. Run from code/:  uv run python ch03/figures.py
"""
import re
import statistics
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.svgfig_bg import write, text, rect

OUT = Path(__file__).resolve().parent / "output"


def runs(pattern, rx):
    vals = []
    for f in sorted(OUT.glob(pattern)):
        m = re.search(rx, f.read_text())
        vals.append(float(m[1]))
    return vals


py_rdd = runs("rdd_vs_df-run*.txt", r"rdd \(Python lambdas\)\s+([\d.]+) s")
py_df = runs("rdd_vs_df-run*.txt", r"\n  dataframe\s+([\d.]+) s")
sc_all = runs("RddVsDf-run*.txt", r"scala rdd \(all 21 columns\)\s+([\d.]+) s")
sc_sel = runs("RddVsDf-run*.txt", r"scala rdd \(select 2 columns first\)\s+([\d.]+) s")
sc_df = runs("RddVsDf-run*.txt", r"scala dataframe\s+([\d.]+) s")


def times(c):
    rows = [("Python RDD (lambdas over Row objects)", py_rdd, c["acc"]),
            ("Scala RDD, all 21 columns", sc_all, c["purple"]),
            ("Scala RDD, 2 columns selected first", sc_sel, c["purple"]),
            ("Scala DataFrame", sc_df, c["blue"]),
            ("Python DataFrame", py_df, c["blue"])]
    W, L, top = 1000, 300, max(max(r[1]) for r in rows)
    scale = (W - L - 190) / top
    out = [text(20, 26, "Trips and average fare by hour, 3.8 million rows: seconds (median of each run)", c, 15, c["fg"], 700, "start")]
    for i, (label, vals, col) in enumerate(rows):
        y = 50 + i * 46
        med = statistics.median(vals)
        out.append(text(20, y + 21, label, c, 13, c["fg"], 500, "start"))
        out.append(rect(L, y + 6, max(med * scale, 2), 22, c, fill=col, stroke=col, r=3))
        lo, hi = min(vals), max(vals)
        out.append(f'<line x1="{L + lo * scale:.1f}" y1="{y + 17}" x2="{L + hi * scale:.1f}" y2="{y + 17}" stroke="{c["fg"]}" stroke-width="1.5"/>')
        out.append(text(L + max(med, hi) * scale + 8, y + 22, f"{med:.2f} s  ({lo:.2f} to {hi:.2f})", c, 12, c["mute"], 400, "start"))
    out.append(text(20, 50 + 5 * 46 + 16, "Bars: median of the three run medians. Lines: the range over three runs. Lower is better.", c, 12, c["mute"], 400, "start"))
    return out


tung = (OUT / "tungsten.txt").read_text()
words = re.findall(r"word (\d): +(\w+)", tung)


def unsafe_row(c):
    labels = ["null bitset", "VendorID = 1", "trip_distance = 2.9", "offset 40, length 1",
              "fare_amount = 16.3", "\"N\" (variable part)"]
    sub = ["4 fields, no nulls", "int in an 8-byte slot", "IEEE 754 double", "store_and_fwd_flag",
           "IEEE 754 double", "padded to 8 bytes"]
    cols = [c["grey_soft"], c["blue_soft"], c["blue_soft"], c["acc_soft"], c["blue_soft"], c["green_soft"]]
    out = [text(20, 26, "One taxi row as an UnsafeRow: 48 bytes, six 8-byte words (tungsten.txt)", c, 15, c["fg"], 700, "start")]
    for i, (n, w) in enumerate(words):
        x = 20 + i * 160
        out.append(rect(x, 50, 150, 70, c, fill=cols[i], stroke=c["line"], r=4))
        out.append(text(x + 75, 72, f"bytes {i * 8}-{i * 8 + 7}", c, 11, c["mute"]))
        out.append(text(x + 75, 98, f"0x{w}", c, 12, c["fg"], 600, mono=True))
        out.append(text(x + 75, 142, labels[i], c, 12, c["fg"], 600))
        out.append(text(x + 75, 160, sub[i], c, 11, c["mute"]))
    out.append(f'<path d="M{20 + 3 * 160 + 75},168 C{20 + 3 * 160 + 75},200 {20 + 5 * 160 + 75},200 {20 + 5 * 160 + 75},168" '
               f'fill="none" stroke="{c["acc"]}" stroke-width="1.6" stroke-dasharray="4 3"/>')
    out.append(text(20 + 4 * 160 + 75, 214, "the slot of a string holds (offset << 32) | length", c, 12, c["acc"], 600))
    return out


write("ch03-times", 1000, 300, times)
write("ch03-unsaferow", 1000, 220, unsafe_row)
print(py_rdd, py_df, sc_all, sc_sel, sc_df, words)
