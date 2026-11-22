"""Figures for Chapter 19. Run: python3 ch19/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit import Fig


def memory():
    # output/memory.txt, defaults row: heap 1024 MiB, pool 434.4 MiB; 16 tasks (local[*], 16 cores)
    heap, res, frac = 1024, 300, 0.6
    pool = (heap - res) * frac; user = (heap - res) - pool
    f = Fig(860, 300, "The regions of the default 1 GiB heap")
    f.text(20, 22, "Default driver heap in local mode: 1024 MiB (memory.txt)", size=14, anchor="start", weight=700)
    X, W, Y, H = 20, 820, 50, 64
    sc = W / heap
    segs = [("reserved", res, "mute", "300 MiB"), ("user memory", user, "blue", f"{user:.1f} MiB"),
            ("execution", pool / 2, "acc", f"{pool/2:.1f} MiB"), ("storage", pool / 2, "ok", f"{pool/2:.1f} MiB")]
    x = X
    for name, v, k, lab in segs:
        f.box(x, Y, v * sc, H, name, lab, kind={"mute": "mute", "blue": "blue", "acc": "accent", "ok": "ok"}[k], size=14, rx=4)
        x += v * sc
    # brace for unified pool
    px0 = X + (res + user) * sc
    f.path(f"M{px0},{Y+H+10} L{px0},{Y+H+20} L{X+W},{Y+H+20} L{X+W},{Y+H+10}", color="acc", sw=1.6)
    f.text((px0 + X + W) / 2, Y + H + 38, f"unified pool = (1024 - 300) x 0.6 = {pool:.1f} MiB", size=13.5, color="acc", weight=600)
    f.line(px0 + pool / 2 * sc, Y - 6, px0 + pool / 2 * sc, Y + H + 6, color="fg", sw=1.5, dash="4 3")
    f.text(px0 + pool / 2 * sc, Y - 12, "storageFraction 0.5 (soft line)", size=11.5, color="mute")
    # per task
    f.text(20, 190, "16 tasks run at the same time and share the execution memory:", size=13.5, anchor="start")
    lo, hi = pool / 32, pool / 16
    f.text(20, 214, f"each task gets between 1/(2N) and 1/N of it: {lo:.1f} MiB to {hi:.1f} MiB", size=13.5, anchor="start", weight=600)
    for i in range(16):
        f.box(20 + i * 51, 236, 45, 26, kind="accent", rx=3)
        f.text(20 + i * 51 + 22.5, 249, f"t{i+1}", size=11, color="fg")
    f.text(20, 284, "A task that needs more than its share spills (Step 7).", size=12.5, anchor="start", color="mute")
    f.save("ch19-memory")


def coalesce1():
    # output/repartition.txt: median of 5 runs
    rows = [("no change", 17.70, "[17]", "paper"), ("coalesce(1)", 67.99, "[1]", "red"), ("repartition(1)", 19.81, "[1, 17]", "ok")]
    f = Fig(760, 200, "Time of the SHA-256 job with coalesce(1) and repartition(1)")
    f.text(20, 20, "SHA-256 of each row of 22,836,530 trips to the noop sink, median of 5 runs", size=13.5, anchor="start", color="mute")
    x0, mw = 150, 470
    for i, (n, v, t, k) in enumerate(rows):
        y = 44 + i * 50
        f.text(x0 - 12, y + 16, n, size=14, anchor="end", mono=True)
        f.box(x0, y, mw * v / 67.99, 32, kind=k, rx=4)
        f.text(x0 + mw * v / 67.99 + 10, y + 10, f"{v:.2f} s", size=14, anchor="start", weight=700)
        f.text(x0 + mw * v / 67.99 + 10, y + 26, f"tasks per stage {t}", size=11.5, anchor="start", color="mute")
    f.save("ch19-coalesce1")


def spill():
    # output/spill.txt
    rows = [("2", "false", 2, 370, 98), ("16", "false", 16, 391, 104), ("200", "false", 200, 0, 0),
            ("2", "true", 2, 555, 148), ("200", "true", 17, 368, 98)]
    f = Fig(780, 330, "Spill of the June sort for each partition setting")
    f.text(20, 20, "Sort of all June trips, 1 GiB heap: memory spilled and disk spilled (spill.txt)", size=13.5, anchor="start", color="mute")
    x0, mw, mx = 230, 440, 555
    for i, (p, aqe, tasks, mem, disk) in enumerate(rows):
        y = 46 + i * 54
        f.text(x0 - 12, y + 10, f"partitions {p}, AQE {aqe}", size=13, anchor="end", mono=True)
        f.text(x0 - 12, y + 28, f"{tasks} sort tasks", size=11.5, anchor="end", color="mute")
        if mem:
            f.box(x0, y, mw * mem / mx, 18, kind="accent", rx=3)
            f.box(x0, y + 21, mw * disk / mx, 18, kind="blue", rx=3)
            f.text(x0 + mw * mem / mx + 8, y + 9, f"{mem} MiB", size=12.5, anchor="start", weight=600)
            f.text(x0 + mw * disk / mx + 8, y + 30, f"{disk} MiB", size=12.5, anchor="start")
        else:
            f.text(x0 + 4, y + 19, "0 MiB: each task holds about 1 MiB, which fits", size=12.5, anchor="start", color="ok", weight=600)
    ly = 46 + 5 * 54 + 8
    f.box(x0, ly - 7, 14, 14, kind="accent", rx=2); f.text(x0 + 20, ly, "memory spilled", size=12, anchor="start")
    f.box(x0 + 150, ly - 7, 14, 14, kind="blue", rx=2); f.text(x0 + 170, ly, "disk spilled (serialized, compressed)", size=12, anchor="start")
    f.save("ch19-spill")


def rowgroups():
    # output/file_partitions.txt: June = 1 file, 62.4 MB, 4 row groups, split 4.2M, 16 partitions, 4 return rows
    f = Fig(860, 230, "Splits and row groups of the June file")
    f.text(20, 20, "June file: 62.4 MB, 16 splits, 4 row groups. Only a split that holds a row-group midpoint reads that row group.", size=13, anchor="start", color="mute")
    X, W = 20, 820
    # row groups (equal widths: schematic)
    for i in range(4):
        f.box(X + i * W / 4, 44, W / 4, 40, f"row group {i}", "", kind="blue", size=13, rx=3)
        mx = X + i * W / 4 + W / 8
        f.line(mx, 84, mx, 128, color="acc", sw=1.6, dash="4 3")
        f.circle(mx, 84, 4, fill="acc")
    hits = set()
    for i in range(4):
        hits.add(int((i * W / 4 + W / 8) // (W / 16)))
    for j in range(16):
        k = "accent" if j in hits else "ghost"
        f.box(X + j * W / 16 + 2, 128, W / 16 - 4, 36, str(j), "", kind=k, size=12, rx=3, weight=600 if j in hits else 400)
    f.text(X, 186, "split = scan task", size=12.5, anchor="start", color="mute")
    f.box(X + 150, 179, 14, 14, kind="accent", rx=2); f.text(X + 170, 186, "4 tasks return rows", size=12.5, anchor="start")
    f.box(X + 330, 179, 14, 14, kind="ghost", rx=2); f.text(X + 350, 186, "12 tasks return no rows", size=12.5, anchor="start")
    f.note("Schematic: equal row-group sizes. The counts (16 splits, 4 with rows) are from file_partitions.txt.", y=216)
    f.save("ch19-rowgroups")


if __name__ == "__main__":
    memory(); coalesce1(); spill(); rowgroups()
