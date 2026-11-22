"""Figures for the ORC and encryption steps of Chapter 36. Run: python3 ch36/figures_orc.py (from code/).
Numbers are parsed from output/orc_dump.txt, output/orc_compare.txt and output/crypto_rebase.txt."""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig

OUT = Path(__file__).resolve().parent / "output"


def orc_bytemap():
    txt = (OUT / "orc_dump.txt").read_text()
    total = int(re.search(r"\.\.\.: ([\d,]+) bytes", txt).group(1).replace(",", ""))
    st = re.search(r"stripe 1: offset (\d+), index (\d+), data (\d+), footer (\d+), rows (\d+)", txt)
    off, idx, data, sfoot, rows = map(int, st.groups())
    ps = re.search(r"metadata (\d+) B \| footer (\d+) B \| postscript (\d+) B", txt)
    meta, foot, pslen = map(int, ps.groups())
    segs = [(0, 3, "", "mute"), (off, off + idx, "row\nindex", "warn"), (off + idx, off + idx + data, "stripe data\n(column streams)", "blue"),
            (off + idx + data, off + idx + data + sfoot, "", "accent")]
    a = off + idx + data + sfoot
    segs += [(a, a + meta, "", "ok"), (a + meta, a + meta + foot, "footer", "ok"), (a + meta + foot, total - 1, "", "red"), (total - 1, total, "", "mute")]
    f = Fig(900, 260, "Byte map of the zones ORC file")
    f.text(20, 20, f"The zones file as ORC, {total:,} bytes, drawn to scale (orc_dump.txt)", size=13.5, anchor="start", color="mute")
    X, W, Y, H = 20, 860, 62, 64
    sc = W / total
    for s0, s1, lab, k in segs:
        f.box(X + s0 * sc, Y, max(1.5, (s1 - s0) * sc), H, lab, "", kind=k, size=12, rx=1, weight=600)
    for o in (off, off + idx, a, a + meta, a + meta + foot):
        f.line(X + o * sc, Y + H, X + o * sc, Y + H + 10, color="mute")
    for o, dy in ((off + idx, 0), (a, 14), (a + meta + foot, 0)):
        f.text(X + o * sc, Y + H + 20 + dy, f"{o:,}", size=11, color="mute", mono=True)
    f.text(X, Y - 12, "ORC", size=11.5, anchor="start")
    f.text(X + (a + meta / 2) * sc, Y - 30, f"metadata (stripe statistics) {meta} B", size=11.5, anchor="end")
    f.text(X + W, Y - 12, f"postscript {pslen} B + 1 length byte", size=11.5, anchor="end")
    ly = 196
    items = [("warn", "row index (min, max per 10,000 rows)"), ("blue", "column data"), ("accent", "stripe footer"),
             ("ok", "metadata and footer (protobuf)"), ("red", "postscript")]
    x = 20
    for k, t in items:
        f.box(x, ly - 7, 14, 14, kind=k, rx=2); f.text(x + 20, ly, t, size=12, anchor="start"); x += 40 + len(t) * 6.4
    f.text(20, 232, "A reader reads the last byte, then the postscript, then the footer, then only the streams that the query needs.",
           size=12.5, anchor="start")
    f.save("ch36-orc-bytemap")


if __name__ == "__main__":
    orc_bytemap()
