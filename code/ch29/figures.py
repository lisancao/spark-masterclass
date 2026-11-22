"""Figures for Chapter 29. Run: python3 ch29/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit import Fig


def versions():
    # output/c1_hdfs.txt: decoded deltas and snapshots of operator 0
    p = {0: {"1.delta": [("u1", 3), ("u4", 2)], "2.delta": [("u5", 2), ("u4", 3)], "3.delta": [("u5", 4), ("u4", 4), ("u1", 5)],
             "3.snapshot": [("u5", 4), ("u4", 4), ("u1", 5)], "4.delta": [("u1", 6), ("u7", 1)]},
         1: {"1.delta": [("u2", 3), ("u3", 2)], "2.delta": [("u3", 5), ("u2", 4)], "3.delta": [("u6", 1), ("u2", 5)],
             "3.snapshot": [("u6", 1), ("u2", 5), ("u3", 5)], "4.delta": [("u6", 3)]}}
    cols = ["1.delta", "2.delta", "3.delta", "3.snapshot", "4.delta"]
    f = Fig(900, 330, "Delta files and snapshots of the count, decoded")
    f.text(20, 20, "Operator 0 (the count), decoded by lz4delta.py (c1_hdfs.txt). Batch N-1 writes N.delta.", size=13, anchor="start", color="mute")
    X0, cw = 120, 150
    for j, c in enumerate(cols):
        f.text(X0 + j * cw + cw / 2, 50, c, size=13, weight=700, mono=True, color="ok" if "snapshot" in c else "fg")
        if "delta" in c:
            f.text(X0 + j * cw + cw / 2, 66, f"batch {int(c[0]) - 1}", size=11, color="mute")
    for i in (0, 1):
        y0 = 84 + i * 104
        f.text(X0 - 14, y0 + 40, f"partition {i}", size=13, anchor="end", weight=600)
        for j, c in enumerate(cols):
            recs = p[i][c]
            k = "ok" if "snapshot" in c else "paper"
            hl = (i == 0 and c in ("3.snapshot", "4.delta"))
            f.box(X0 + j * cw + 6, y0, cw - 12, 92, "", "", kind="accent" if hl else k, rx=6)
            for r, (u, n) in enumerate(recs):
                f.text(X0 + j * cw + cw / 2, y0 + 20 + r * 24, f"{u} = {n}", size=13.5, mono=True)
    f.text(X0, 304, "Version 4 of partition 0 after a restart = 3.snapshot + 4.delta: u1 = 6, u4 = 4, u5 = 4, u7 = 1 (orange cells).", size=12.5, anchor="start")
    f.save("ch29-versions")


def deltabytes():
    # 0/1.delta, uncompressed 100 bytes: two records (keySize, key, valueSize, value) and the end marker
    f = Fig(900, 250, "The 100 bytes of 0/1.delta after LZ4")
    f.text(20, 20, "0/1.delta after decompression: 100 bytes, drawn to scale (c1_hdfs.txt, lz4delta.py)", size=13, anchor="start", color="mute")
    X0, W, Y = 20, 860, 70
    sc = W / 100
    segs = []
    for u, n in (("u1", 3), ("u4", 2)):
        segs += [("len\n24", 4, "mute"), (f"key: UnsafeRow\n\"{u}\"", 24, "blue"), ("len\n16", 4, "mute"), (f"value\ncount = {n}", 16, "ok")]
    segs.append(("-1", 4, "red"))
    x = X0
    for lab, n, k in segs:
        f.box(x, Y, n * sc, 56, lab, "", kind=k, size=11, rx=2, weight=600 if n > 4 else 400)
        x += n * sc
    # inside the first key: null bitset 8, slot 8, string bytes 8
    kx = X0 + 4 * sc
    for i, (lab, n) in enumerate((("null bits", 8), ("offset, length", 8), ("'u1' + pad", 8))):
        f.box(kx + i * 8 * sc, Y + 90, 8 * sc - 2, 34, lab, "", kind="blue", size=10.5, rx=2, weight=400)
    f.path(f"M{kx},{Y+56} L{kx},{Y+90}", color="blue", sw=1, dash="3 3")
    f.path(f"M{kx + 24 * sc},{Y+56} L{kx + 24 * sc},{Y+90}", color="blue", sw=1, dash="3 3")
    vx = X0 + 32 * sc
    for i, (lab, n) in enumerate((("null bits", 8), ("LONG 3", 8))):
        f.box(vx + i * 8 * sc, Y + 90, 8 * sc - 2, 34, lab, "", kind="ok", size=10.5, rx=2, weight=400)
    f.path(f"M{vx},{Y+56} L{vx},{Y+90}", color="ok", sw=1, dash="3 3")
    f.path(f"M{vx + 16 * sc},{Y+56} L{vx + 16 * sc},{Y+90}", color="ok", sw=1, dash="3 3")
    f.text(X0, 230, "len = an int with the size of the next row. 2 x (4 + 24 + 4 + 16) + 4 = 100 bytes. Ints are big-endian (DataOutputStream). -1 marks the end of the file.", size=12.5, anchor="start")
    f.save("ch29-deltabytes")


def repartition():
    # output/c3_repartition.txt
    before = {"u1": 0, "u2": 1, "u3": 1, "u4": 0, "u5": 0}
    after = {"u1": 2, "u2": 3, "u3": 1, "u4": 2, "u5": 2}
    f = Fig(820, 316, "Keys before and after the offline repartition")
    f.text(20, 20, "The state of the stopped query, 2 partitions to 4 (c3_repartition.txt). Batch 2 has no input.", size=13, anchor="start", color="mute")
    lx, rx = 160, 560
    f.text(lx + 50, 50, "after batch 1: 2 partitions", size=13, weight=700)
    f.text(rx + 50, 50, "after batch 2: 4 partitions", size=13, weight=700)
    ly = {p: 70 + p * 110 for p in (0, 1)}
    ry = {p: 70 + p * 55 for p in range(4)}
    for p, y in ly.items():
        f.box(lx, y, 100, 100, f"p{p}", "", kind="paper", size=13, rx=6)
    for p, y in ry.items():
        f.box(rx, y, 100, 48, f"p{p}", "", kind="paper", size=13, rx=6)
    cnt = {0: 0, 1: 0}
    rc = {p: 0 for p in range(4)}
    for u in sorted(before):
        b, a = before[u], after[u]
        y1 = ly[b] + 18 + cnt[b] * 26; cnt[b] += 1
        y2 = ry[a] + 12 + rc[a] * 12; rc[a] += 1
        f.text(lx - 10, y1, u, size=12.5, anchor="end", mono=True, weight=600)
        f.arrow(lx + 104, y1, rx - 4, y2, color="acc", sw=1.3)
        f.text(rx + 108, y2, u, size=11.5, anchor="start", mono=True)
    f.text(20, 304, "Each key moves to the partition that its hash gives for 4 partitions. The counts do not change.", size=12.5, anchor="start")
    f.save("ch29-repartition")


if __name__ == "__main__":
    versions(); deltabytes(); repartition()
