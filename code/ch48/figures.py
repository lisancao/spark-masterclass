"""Figures for Chapter 48. Run: python3 ch48/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig

OUT = Path(__file__).resolve().parent / "output"
mins = lambda s: int(s[:2]) * 60 + int(s[3:5])


def windows():
    # output/s5_windows.txt: trip 0610-00100, pickup 07:52:00
    f = Fig(820, 290, "Tumbling, sliding and offset windows of one trip")
    f.text(20, 20, "Trip 0610-00100, pickup 07:52:00, in three kinds of 10-minute window (s5_windows.txt)", size=13, anchor="start", color="mute")
    T0, T1, X0, X1 = mins("07:40"), mins("08:05"), 200, 790
    x = lambda t: X0 + (t - T0) / (T1 - T0) * (X1 - X0)
    rows = [("tumbling", "10 minutes", [("07:50", "08:00")], "accent"),
            ("sliding", "10 minutes, slide 5", [("07:50", "08:00"), ("07:45", "07:55")], "blue"),
            ("offset", "startTime 3 minutes", [("07:43", "07:53")], "ok")]
    y = 52
    for name, sub, wins, k in rows:
        f.text(X0 - 14, y + 16, name, size=14, anchor="end", weight=700)
        f.text(X0 - 14, y + 33, sub, size=11.5, anchor="end", color="mute")
        for i, (a, b) in enumerate(wins):
            f.box(x(mins(a)), y + i * 26, x(mins(b)) - x(mins(a)), 22, f"{a} to {b}", "", kind=k, size=11.5, rx=4)
        y += 26 * len(wins) + 26
    base = y + 6
    f.line(X0, base, X1, base, color="mute")
    for t in range(T0, T1 + 1, 5):
        f.line(x(t), base, x(t), base + 6, color="mute")
        f.text(x(t), base + 18, f"{t // 60:02d}:{t % 60:02d}", size=11, color="mute")
    px = x(mins("07:52"))
    f.line(px, 44, px, base, color="fg", sw=1.6, dash="4 3")
    f.text(px, base + 36, "pickup 07:52", size=12.5, weight=700)
    f.save("ch48-windows")


def lateness():
    # output/s5_windows.txt: counts by tumbling window over the first three files (dropoffs 08:00 to 08:15)
    rows = [("06:30", 1), ("06:40", 0), ("06:50", 7), ("07:00", 14), ("07:10", 12), ("07:20", 41), ("07:30", 126), ("07:40", 301), ("07:50", 574), ("08:00", 343), ("08:10", 26)]
    f = Fig(760, 270, "Pickup windows of the trips that ended from 08:00 to 08:15")
    f.text(20, 20, "Trips in the first three files (dropoffs 08:00 to 08:15), counted by 10-minute pickup window (s5_windows.txt)", size=12.5, anchor="start", color="mute")
    X0, bw, H, base = 60, 58, 170, 220
    for i, (s, c) in enumerate(rows):
        h = H * c / 574
        k = "accent" if s >= "08:00" else "blue"
        if c:
            f.box(X0 + i * bw + 4, base - h, bw - 8, h, kind=k, rx=3)
        f.text(X0 + i * bw + bw / 2, base - h - 10, str(c), size=12, weight=600)
        f.text(X0 + i * bw + bw / 2, base + 14, s, size=11.5, color="mute")
    f.line(X0, base, X0 + len(rows) * bw, base, color="mute")
    f.text(X0 + 4, 60, "blue: pickups before 08:00", size=12, anchor="start", color="blue")
    f.text(X0 + 4, 78, "orange: pickups from 08:00", size=12, anchor="start", color="acc")
    f.text(X0, base + 34, "pickup window start (no trips from 06:40 to 06:50)", size=11.5, anchor="start", color="mute")
    f.save("ch48-lateness")


def delay():
    # output/s6_watermark.txt and s6_delays.txt
    rows = [(5, 13, 1458), (15, 12, 513), (30, 10, 166), (60, 7, 20)]
    f = Fig(780, 250, "The watermark delay trade-off")
    f.text(20, 20, "The same stream with four watermark delays (s6_watermark.txt, s6_delays.txt)", size=13, anchor="start", color="mute")
    X0, mw = 140, 300
    f.text(X0 + mw / 2, 46, "trips that arrived too late", size=12.5, weight=700, color="red")
    f.text(X0 + mw + 190, 46, "windows written by the end", size=12.5, weight=700, color="blue")
    for i, (d, w, late) in enumerate(rows):
        y = 62 + i * 42
        f.text(X0 - 14, y + 14, f"delay {d} min", size=13.5, anchor="end", mono=True)
        f.box(X0, y, max(3, mw * late / 1458), 28, kind="red", rx=3)
        f.text(X0 + max(3, mw * late / 1458) + 8, y + 14, f"{late:,}", size=13, anchor="start", weight=600)
        for j in range(w):
            f.box(X0 + mw + 80 + j * 17, y + 4, 13, 20, kind="blue", rx=2)
        f.text(X0 + mw + 80 + 13 * 17 + 6, y + 14, str(w), size=13, anchor="start", weight=600)
    f.text(20, 236, "A short delay writes results early and drops more trips. A long delay drops few trips and writes later.", size=12.5, anchor="start")
    f.save("ch48-delay")


def outer_join():
    # output/s10b_join_state.txt: per batch, watermark and the pickups of the NULL rows written
    rows = [(4, "08:04", 8, "07:00", "07:04"), (5, "08:09", 3, "07:05", "07:07"), (6, "08:14", 10, "07:10", "07:14"),
            (7, "08:19", 14, "07:15", "07:19"), (8, "08:24", 7, "07:20", "07:23"), (9, "08:29", 7, "07:25", "07:29"),
            (10, "08:34", 12, "07:30", "07:34"), (11, "08:39", 12, "07:35", "07:39"), (12, "08:44", 24, "07:40", "07:44"),
            (13, "08:49", 21, "07:44", "07:49")]
    f = Fig(860, 330, "When a left outer stream-stream join writes its NULL rows")
    f.text(20, 22, "Lost dropoffs: each pickup with no partner goes out when the watermark is 1 hour past it (s10b_join_state.txt)",
           size=12.5, anchor="start", color="mute")
    T0, T1, X0, X1 = mins("07:00"), mins("09:00"), 110, 830
    x = lambda t: X0 + (t - T0) / (T1 - T0) * (X1 - X0)
    base = 250
    f.line(X0, base, X1, base, color="mute")
    for t in range(T0, T1 + 1, 15):
        f.line(x(t), base, x(t), base + 5, color="mute")
        f.text(x(t), base + 18, f"{t // 60:02d}:{t % 60:02d}", size=11, color="mute")
    f.text(X0 - 10, 70, "NULL rows", size=12, anchor="end", color="acc", weight=700)
    f.text(X0 - 10, 190, "watermark", size=12, anchor="end", color="blue", weight=700)
    for i, (b, wm, n, lo, hi) in enumerate(rows):
        y = 52 + (i % 2) * 22
        f.box(x(mins(lo)), y, max(6, x(mins(hi)) - x(mins(lo))), 16, kind="accent", rx=2)
        f.text(x(mins(lo)), y + 30 if i % 2 else y - 6, f"b{b}: {n}", size=10.5, anchor="start", color="acc")
        f.circle(x(mins(wm)), 190, 5, fill="blue")
        f.path(f"M{x(mins(wm)):.1f},184 L{x(mins(lo)) + 3:.1f},{y + 18}", color="line", sw=1, dash="3 3")
    f.box(x(mins("07:49")), 120, x(mins("08:59")) - x(mins("07:49")), 30, "left state at the end: 5,624 pickups (07:49 to 08:59)", kind="paper", size=11.5, rx=3)
    f.box(x(mins("08:49")), 205, x(mins("08:59")) - x(mins("08:49")), 22, "right", kind="blue", size=10.5, rx=3)
    f.text(x(mins("08:49")) - 6, 222, "right state: 1,115 dropoffs", size=11, anchor="end", color="blue")
    f.note("118 NULL rows written; the other lost pickups (after 07:49) wait in state for a later watermark", y=318)
    f.save("ch48-outer-join")


def static_refresh():
    # output/s12_stream_static.txt
    f = Fig(860, 300, "A stream-static join on a folder that changes")
    f.text(20, 22, "Labels found in each batch (s12_stream_static.txt)", size=12.5, anchor="start", color="mute")
    cols = ["batch 0: v1 has two airport zones", "batch 1: new file v2 added", "batch 2: v1 rewritten"]
    for j, c in enumerate(cols):
        f.text(330 + j * 180, 56, c, size=11.5, color="mute")
    rows = [("a) static DataFrame in the query", ["airport", "airport only", "FILE_NOT_EXIST"], ["ok", "warn", "red"]),
            ("b) read again in foreachBatch", ["airport", "airport, midtown, uptown", "+ times-sq"], ["ok", "ok", "ok"])]
    for i, (label, cells, kinds) in enumerate(rows):
        y = 80 + i * 90
        f.text(230, y + 28, label, size=13, anchor="end", weight=700)
        for j, (cell, k) in enumerate(zip(cells, kinds)):
            f.box(250 + j * 180, y, 165, 56, cell, "", kind=k, size=12, rx=6)
    f.text(20, 268, "a) keeps the file list of the moment it was made. b) lists the folder again at the start of each batch.", size=12.5, anchor="start")
    f.save("ch48-static-refresh")


def async_tracking():
    # output/s15_async.txt (medians, 3 runs each)
    rows = [("sync", 32, 32, 96, 64, 8.6), ("async", 0, 0, 32, 4, 4.2)]
    f = Fig(860, 260, "Synchronous and asynchronous progress tracking")
    f.text(20, 22, "Kafka to Kafka, 64 batches of 100 records, medians of 3 runs (s15_async.txt)", size=12.5, anchor="start", color="mute")
    X0, sc = 140, 5.0
    f.text(X0, 50, "median batch (ms): walCommit + commitOffsets + the rest", size=12.5, anchor="start", weight=700)
    for i, (m, wal, com, trig, files, wall) in enumerate(rows):
        y = 66 + i * 50
        f.text(X0 - 12, y + 18, m, size=14, anchor="end", weight=700, mono=True)
        rest = trig - wal - com
        xx = X0
        for v, k, lab in ((wal, "warn", "walCommit"), (com, "red", "commit"), (rest, "blue", "")):
            if v:
                f.box(xx, y, v * sc, 32, f"{lab} {v}" if lab else "", "", kind=k, size=11, rx=3)
                xx += v * sc
        f.text(xx + 8, y + 18, f"{trig} ms", size=13, anchor="start", weight=700)
        f.text(720, y + 18, f"{files} offset files, {wall} s", size=12.5, anchor="start", color="mute")
    f.text(20, 200, "The two checkpoint writes leave the batch. The background thread writes one entry per second.", size=12.5, anchor="start")
    f.text(20, 222, "Times from one laptop that other jobs also used; the ratio is the result.", size=12, anchor="start", color="mute")
    f.save("ch48-async")


if __name__ == "__main__":
    windows(); lateness(); delay(); outer_join(); static_refresh(); async_tracking()
