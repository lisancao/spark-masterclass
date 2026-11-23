"""Figures for Chapter 36. Run: python3 ch36/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig


def bytemap():
    # output/footer.txt: 5,956 bytes; page offsets; footer at 4,978, length 970
    total = 5956
    segs = [(0, 4, "", "mute"), (4, 1101, "LocationID\ndata page", "blue"), (1101, 1207, "", "warn"), (1207, 1347, "", "accent"),
            (1347, 4585, "Zone\ndata page (PLAIN)", "blue"), (4585, 4658, "", "warn"), (4658, 4978, "", "accent"),
            (4978, 5948, "footer\n970 bytes", "ok"), (5948, 5952, "", "mute"), (5952, 5956, "", "mute")]
    f = Fig(900, 256, "Byte map of the zones Parquet file")
    f.text(20, 20, "The zones file, 5,956 bytes, drawn to scale (footer.txt)", size=13.5, anchor="start", color="mute")
    X, W, Y, H = 20, 860, 60, 64
    sc = W / total
    for a, b, lab, k in segs:
        f.box(X + a * sc, Y, max(1.5, (b - a) * sc), H, lab, "", kind=k, size=12, rx=1, weight=600)
    # offsets
    for i, off in enumerate((4, 1101, 1347, 4585, 4978)):
        dy = 14 if off in (1347, 4978) else 0
        f.line(X + off * sc, Y + H, X + off * sc, Y + H + 10 + dy, color="mute")
        f.text(X + off * sc, Y + H + 20 + dy, f"{off:,}", size=11, color="mute", mono=True)
    # callouts for small segments
    f.text(X, Y - 12, "PAR1", size=11.5, anchor="start", color="fg")
    f.text(X + 1101 * sc + 10, Y - 12, "Borough: dictionary + data", size=11.5, color="fg")
    f.text(X + 4585 * sc + 10, Y - 12, "service_zone: dict. + data", size=11.5, color="fg")
    f.text(X + W - 4, Y - 12, "length + PAR1", size=11.5, anchor="end", color="fg")
    ly = 196
    items = [("blue", "plain data page"), ("warn", "dictionary page"), ("accent", "dictionary-encoded data page"), ("ok", "footer (FileMetaData)"), ("mute", "magic bytes, footer length")]
    x = 20
    for k, t in items:
        f.box(x, ly - 7, 14, 14, kind=k, rx=2); f.text(x + 20, ly, t, size=12, anchor="start"); x += 40 + len(t) * 6.6
    f.text(20, 230, "A reader reads the last 8 bytes, then the footer, then only the column chunks that the query needs.", size=12.5, anchor="start")
    f.save("ch36-bytemap")


def skipping():
    # output/skipping.txt: rows the reader returned, of 2,000,000
    qs = ["user_id = probe", "event = 'refund'", "session_id = probe", "ts in one hour"]
    data = [[2000000, 20000, 15360, 15360], [2000000, 242200, 15272, 15272], [2000000, 2000000, 2000000, 15360], [2000000] * 4]
    lays = ["A: as generated", "B: sorted by user_id", "C: B + 1 MB row groups", "D: C + bloom filter"]
    f = Fig(860, 290, "Rows that the Parquet reader returned, per layout and query")
    f.text(20, 20, "Rows that the reader returned, of 2,000,000 (skipping.txt). Fewer rows means more skipped data.", size=13, anchor="start", color="mute")
    cx, cw, ry, rh = 190, 165, 70, 46
    for j, l in enumerate(lays):
        f.text(cx + j * cw + cw / 2, 52, l, size=12.5, weight=600)
    for i, q in enumerate(qs):
        y = ry + i * rh
        f.text(cx - 12, y + rh / 2, q, size=12.5, anchor="end", mono=True)
        for j in range(4):
            v = data[i][j]
            k = "red" if v == 2000000 else ("warn" if v > 100000 else "ok")
            f.box(cx + j * cw + 3, y + 3, cw - 6, rh - 6, f"{v:,}", "", kind=k, size=13.5, rx=4, weight=600 if k == "ok" else 400)
    ly = ry + 4 * rh + 20
    for x, k, t in [(cx, "red", "nothing skipped"), (cx + 170, "warn", "some files skipped"), (cx + 360, "ok", "one row group or one page")]:
        f.box(x, ly - 7, 14, 14, kind=k, rx=2); f.text(x + 20, ly, t, size=12, anchor="start")
    f.save("ch36-skipping")


def codecs():
    # output/skipping_codecs.txt
    rows = [("none", 108.2, 5.1, 0.91), ("snappy (default)", 76.8, 4.3, 0.78), ("lz4_raw", 76.2, 4.3, 0.70), ("gzip", 59.2, 7.5, 0.90), ("zstd", 58.2, 5.8, 0.84)]
    f = Fig(780, 290, "File size of the June trips for each codec")
    f.text(20, 20, "The June trips as one file with each codec: size, write time, full read time (skipping_codecs.txt)", size=13, anchor="start", color="mute")
    x0, mw = 160, 380
    for i, (n, mb, w, r) in enumerate(rows):
        y = 44 + i * 44
        f.text(x0 - 12, y + 14, n, size=13.5, anchor="end", mono=True)
        k = "accent" if n.startswith("snappy") else "blue"
        f.box(x0, y, mw * mb / 108.2, 28, kind=k, rx=4)
        f.text(x0 + mw * mb / 108.2 + 10, y + 14, f"{mb} MB", size=13.5, anchor="start", weight=700)
        f.text(x0 + mw * mb / 108.2 + 80, y + 14, f"write {w} s, read {r:.2f} s", size=12, anchor="start", color="mute")
    f.text(20, 275, "The read times differ by less than the noise of these runs.", size=12.5, anchor="start")
    f.save("ch36-codecs")


if __name__ == "__main__":
    bytemap(); skipping(); codecs()
