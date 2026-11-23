"""Figures for Chapter 44. Run: python3 ch44/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig


def deltalog():
    # output/delta_log.txt: versions 0 to 2, their add/remove actions, rows per file, delta vs parquet read
    f = Fig(860, 330, "Delta versions and the files on disk")
    f.text(20, 20, "The employees table after WRITE, MERGE and DELETE (delta_log.txt)", size=13, anchor="start", color="mute")
    files = [("part-00000-3fafd84...", 3), ("part-00000-66fcbc5...", 5), ("part-00000-2ff2851...", 4)]
    vers = [("00000000000000000000.json", "WRITE", [("add", 0)]), ("00000000000000000001.json", "MERGE", [("remove", 0), ("add", 1)]),
            ("00000000000000000002.json", "DELETE", [("remove", 1), ("add", 2)])]
    fx = 560
    f.text(fx + 130, 52, "data files on disk", size=13, weight=700)
    for i, (n, r) in enumerate(files):
        f.box(fx, 70 + i * 70, 260, 52, f"{'ABC'[i]}: {n}", f"{r} rows", kind="ok" if i == 2 else "mute", size=12, mono=True, subsize=11.5, submono=False)
    f.text(160, 52, "_delta_log/", size=13, weight=700, mono=True)
    for v, (n, op, acts) in enumerate(vers):
        y = 70 + v * 70
        f.box(20, y, 360, 52, f"version {v}: {op}", n + "  |  " + ", ".join(f"{a} {'ABC'[fi]}" for a, fi in acts), kind="blue", size=13, subsize=10.5, submono=True)
        for a, fi in acts:
            ty = 70 + fi * 70 + 26
            col = "ok" if a == "add" else "red"
            f.arrow(382, y + 26, fx - 4, ty, color=col, sw=1.4, dash=None if a == "add" else "4 3")
    f.box(20, 290 - 18, 360, 36, "read as delta: 4 rows (file C only)", "", kind="ok", size=13)
    f.box(400, 290 - 18, 440, 36, "read as parquet: 12 rows (every file still on disk)", "", kind="red", size=13)
    f.save("ch44-deltalog")


def cow_mor():
    # output/iceberg_log.txt: copy-on-write (overwrite snapshots) and merge-on-read (position delete file)
    f = Fig(860, 260, "Copy-on-write and merge-on-read in Iceberg")
    f.text(20, 20, "A DELETE of one row in each mode (iceberg_log.txt)", size=13, anchor="start", color="mute")
    f.text(210, 52, "copy-on-write: DELETE Charlie", size=13.5, weight=700)
    f.box(20, 70, 160, 56, "data file", "5 rows", kind="mute", size=13)
    f.arrow(184, 98, 250, 98, color="mute", label="rewrite", lsize=11)
    f.box(254, 70, 160, 56, "new data file", "4 rows", kind="ok", size=13)
    f.text(210, 150, "overwrite snapshot: 1 file added, 1 deleted", size=12, color="mute")
    f.text(650, 52, "merge-on-read: DELETE Eve", size=13.5, weight=700)
    f.box(470, 70, 190, 56, "data file (same file)", "4 rows, content 0", kind="paper", size=13)
    f.text(675, 98, "+", size=22, weight=700)
    f.box(690, 70, 150, 56, "position delete file", "1 row, content 1", kind="warn", size=12.5)
    f.text(655, 150, "a reader skips the positions in the delete file", size=12, color="mute")
    f.text(20, 200, "Copy-on-write rewrites a whole file for a small change. Merge-on-read writes a small delete file,", size=12.5, anchor="start")
    f.text(20, 220, "and readers do a little more work until compaction writes the deletes into new data files.", size=12.5, anchor="start")
    f.save("ch44-cow-mor")


if __name__ == "__main__":
    deltalog(); cow_mor()
