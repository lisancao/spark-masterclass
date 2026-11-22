"""Figures for Chapter 31. Run: python3 ch31/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig


def maintenance():
    # output/maintenance.txt: data files on disk and live data files after each step
    rows = [("before", 37, 31), ("after 8 INSERTs", 45, 39), ("rewrite_data_files", 47, 3), ("rewrite_manifests", 47, 3),
            ("expire_snapshots", 39, 3), ("drop branch and tag, expire", 3, 3), ("remove_orphan_files", 3, 3)]
    f = Fig(860, 370, "Data files on disk and live data files after each maintenance step")
    f.text(20, 20, "lake.nyc.trips: data files on disk and data files in the current snapshot (maintenance.txt)", size=13, anchor="start", color="mute")
    X0, mw = 230, 520
    for i, (n, disk, live) in enumerate(rows):
        y = 46 + i * 38
        f.text(X0 - 12, y + 13, n, size=12.5, anchor="end", mono=True)
        f.box(X0, y, mw * disk / 47, 14, kind="red", rx=2)
        f.box(X0, y + 15, mw * live / 47, 14, kind="ok", rx=2)
        f.text(X0 + mw * disk / 47 + 8, y + 7, f"{disk} on disk", size=11.5, anchor="start")
        f.text(X0 + mw * live / 47 + 8, y + 22, f"{live} live", size=11.5, anchor="start", color="ok")
    f.text(X0, 46 + 7 * 38 + 14, "rewrite_data_files makes the live table small. Only expire_snapshots removes files from the disk,", size=12, anchor="start")
    f.text(X0, 46 + 7 * 38 + 32, "and a branch or a tag keeps the files of its snapshot.", size=12, anchor="start")
    f.save("ch31-maintenance")


def schemas():
    # output/evolve.txt: four ALTER TABLE statements, field IDs of the current schema
    cols = [6, 8, 9, 10, 11, 12]
    base = {6: "PULocationID int", 8: "payment_type", 9: "fare_amount", 10: "tip_amount", 11: "total_amount", 12: ""}
    steps = [("schema 0", "v2: CREATE + INSERT", {}),
             ("schema 1", "v3: ADD COLUMN tip_pct", {12: "tip_pct"}),
             ("schema 2", "v4: RENAME fare_amount", {9: "fare"}),
             ("schema 3", "v5: PULocationID to BIGINT", {6: "PULocationID long"}),
             ("schema 4", "v6: DROP payment_type", {8: None})]
    f = Fig(960, 300, "Field IDs across the five schemas")
    f.text(20, 20, "The columns that change, by field ID (evolve.txt). Fields 1 to 5 and 7 do not change. No data file changes.", size=12.5, anchor="start", color="mute")
    X0, cw, rh = 230, 120, 40
    for j, c in enumerate(cols):
        f.text(X0 + j * cw + cw / 2, 50, f"field {c}", size=12.5, weight=700)
    cur = dict(base)
    for i, (sid, what, ch) in enumerate(steps):
        y = 64 + i * rh
        f.text(X0 - 12, y + 12, sid, size=12.5, anchor="end", weight=600)
        f.text(X0 - 12, y + 27, what, size=10.5, anchor="end", color="mute", mono=True)
        for k, v in ch.items():
            cur[k] = v
        for j, c in enumerate(cols):
            v = cur[c]
            changed = c in ch
            if v is None:
                f.box(X0 + j * cw + 3, y + 3, cw - 6, rh - 6, "(dropped)", "", kind="red", size=11, rx=4, weight=400)
            elif v == "":
                f.box(X0 + j * cw + 3, y + 3, cw - 6, rh - 6, "", "", kind="ghost", size=11, rx=4, dash="2 3")
            else:
                f.box(X0 + j * cw + 3, y + 3, cw - 6, rh - 6, v, "", kind="accent" if changed else "paper", size=10, rx=4, mono=True, weight=600 if changed else 400)
    f.text(X0, 284, "An old Parquet file still names field 9 fare_amount and stores field 6 as int32. Iceberg reads both by ID.", size=12, anchor="start")
    f.save("ch31-schemas")


def pruning():
    # output/tables.txt and evolve.txt: one day filter on daily June files and on the monthly May file
    f = Fig(860, 230, "Files that one day filter reads in each partition layout")
    f.text(20, 20, "A filter on one day in June (daily spec 0) and in May (monthly spec 1) (tables.txt, evolve.txt)", size=12.5, anchor="start", color="mute")
    X0 = 200
    f.text(X0 - 12, 70, "June, spec 0", size=13, anchor="end", weight=700)
    f.text(X0 - 12, 86, "read 1, skipped 29", size=11.5, anchor="end", color="mute")
    for d in range(30):
        k = "accent" if d == 14 else "ghost"
        f.box(X0 + d * 21, 60, 18, 34, "", "", kind=k, rx=3)
    f.text(X0 + 14 * 21 + 9, 108, "06-15", size=11, color="acc", weight=700)
    f.text(X0 - 12, 150, "May, spec 1", size=13, anchor="end", weight=700)
    f.text(X0 - 12, 166, "read 1, skipped 0", size=11.5, anchor="end", color="mute")
    f.box(X0, 140, 30 * 21 - 3, 34, "one file for the month: read, then filter the rows", "", kind="accent", size=12, rx=3)
    f.text(X0, 208, "In each query, the manifest list skips the manifest of the other month without a read.", size=12, anchor="start")
    f.save("ch31-pruning")


if __name__ == "__main__":
    maintenance(); schemas(); pruning()
