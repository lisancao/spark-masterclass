"""Figures for Chapter 25. Run: python3 ch25/figures.py (from code/). Numbers from output/*.txt."""
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
    f.save("ch25-windows")


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
    f.save("ch25-lateness")


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
    f.save("ch25-delay")


if __name__ == "__main__":
    windows(); lateness(); delay()
