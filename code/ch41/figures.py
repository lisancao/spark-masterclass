"""Figures for Chapter 41. Run: python3 ch41/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig

sec = lambda s: int(s[:2]) * 3600 + int(s[3:5]) * 60 + int(s[6:8])


def sessions():
    # output/t2_sessions.txt: closed sessions, open sessions with timers, batch watermarks
    closed = [("u1", "09:00:14", "09:09:18", 3, "gap"), ("u1", "10:45:56", "10:50:12", 2, "gap"), ("u2", "09:02:03", "09:48:54", 4, "timer"),
              ("u3", "09:30:04", "09:58:49", 5, "timer"), ("u4", "09:38:21", "10:05:44", 3, "gap"), ("u4", "10:58:40", "10:58:40", 1, "timer"),
              ("u5", "10:10:05", "10:44:31", 4, "timer")]
    opened = [("u1", "11:40:50", "11:40:50", 1, "12:10:50"), ("u6", "10:55:05", "11:31:49", 3, "12:01:49"), ("u7", "11:48:03", "11:48:03", 1, "12:18:03")]
    wms = [("1", "09:30:48"), ("2", "10:02:28"), ("3", "10:48:40"), ("4", "11:38:03")]
    users = ["u1", "u2", "u3", "u4", "u5", "u6", "u7"]
    f = Fig(900, 380, "Sessions of the seven users, closed and open")
    f.text(20, 20, "Sessions from t2_sessions.txt. Vertical lines: the watermark that each batch used.", size=13, anchor="start", color="mute")
    T0, T1, X0, X1 = sec("09:00:00"), sec("12:25:00"), 60, 870
    x = lambda s: X0 + (sec(s) - T0) / (T1 - T0) * (X1 - X0)
    top, rh = 70, 34
    for i, u in enumerate(users):
        f.text(X0 - 12, top + i * rh + rh / 2, u, size=13, anchor="end", mono=True, weight=600)
        f.line(X0, top + i * rh + rh / 2, X1, top + i * rh + rh / 2, color="line", sw=0.6, dash="1 4")
    for b, w in wms:
        f.line(x(w), top - 10, x(w), top + 7 * rh, color="acc", sw=1.4, dash="5 3")
        f.text(x(w), top - 18, f"batch {b}: {w[:5]}", size=11, color="acc")
    for u, a, b, n, by in closed:
        i = users.index(u)
        w = max(6, x(b) - x(a))
        f.box(x(a), top + i * rh + 6, w, rh - 12, "", "", kind="blue" if by == "gap" else "ok", rx=4)
        f.text(x(a) + w + 5, top + i * rh + rh / 2, f"{n}, {by}", size=11, anchor="start", color="mute")
    for u, a, b, n, t in opened:
        i = users.index(u)
        w = max(6, x(b) - x(a))
        f.box(x(a), top + i * rh + 6, w, rh - 12, "", "", kind="ghost", rx=4, dash="3 3")
        f.line(x(b), top + i * rh + rh / 2, x(t), top + i * rh + rh / 2, color="mute", sw=1, dash="2 3")
        tx, ty = x(t), top + i * rh + rh / 2
        f.path(f"M{tx},{ty-6} L{tx+6},{ty} L{tx},{ty+6} L{tx-6},{ty} Z", color="fg", fill="fg", sw=1)
    for hh in range(9, 13):
        for mm in (0, 30):
            s = f"{hh:02d}:{mm:02d}:00"
            if T0 <= sec(s) <= T1:
                f.text(x(s), top + 7 * rh + 14, s[:5], size=11, color="mute")
    ly = top + 7 * rh + 40
    items = [("blue", "closed by a click after the gap"), ("ok", "closed by the timer"), ("ghost", "open in state")]
    xx = X0
    for k, t in items:
        f.box(xx, ly - 7, 14, 14, kind=k, rx=2, dash="3 3" if k == "ghost" else None); f.text(xx + 20, ly, t, size=12, anchor="start"); xx += 40 + len(t) * 6.4
    f.path(f"M{xx},{ly-6} L{xx+6},{ly} L{xx},{ly+6} L{xx-6},{ly} Z", color="fg", fill="fg", sw=1)
    f.text(xx + 12, ly, "registered timer", size=12, anchor="start")
    f.save("ch41-sessions")


def clock():
    # output/t3_time.txt (seconds since the script started)
    f = Fig(860, 250, "Processing-time timers and TTL of LastSeen")
    f.text(20, 20, "t3_time.txt: clicks, timer rows and the 8 s TTL of the last page, in seconds since the script started", size=12.5, anchor="start", color="mute")
    X0, X1, T0, T1 = 80, 840, 12.0, 31.0
    x = lambda t: X0 + (t - T0) / (T1 - T0) * (X1 - X0)
    ya, yb = 80, 150
    for y, u in ((ya, "a"), (yb, "b")):
        f.text(X0 - 14, y, f"user {u}", size=13, anchor="end", weight=700)
        f.line(X0, y, X1, y, color="line", sw=0.8)
    # TTL spans from the last update of each value (8 s)
    for t0, t1, y in ((13.4, 16.4, ya), (16.4, 24.4, ya), (17.4, 25.4, yb)):
        f.rect(x(t0), y - 26, x(t1) - x(t0) - 2, 12, fill="okf", stroke="ok", rx=3, sw=1)
    f.text((x(16.4) + x(24.4)) / 2, ya - 36, "TTL of /cart: 8 s from the last update", size=11, color="ok")
    for t, y, lab in ((13.4, ya, "/p/desk"), (16.4, ya, "/cart"), (17.4, yb, "/"), (28.4, ya, "/p/lamp")):
        f.circle(x(t), y, 7, fill="blue")
        f.text(x(t), y + 20, f"{t} s click {lab}", size=11, color="blue")
    for t, y, lab in ((23.6, ya, "timer, saw /cart"), (24.6, yb, "timer, saw /")):
        tx = x(t)
        f.path(f"M{tx},{y-8} L{tx+8},{y} L{tx},{y+8} L{tx-8},{y} Z", color="acc", fill="acc", sw=1)
        f.text(tx, y + 20, f"{t} s {lab}", size=11, color="acc")
    f.text(x(28.4), ya + 36, "state before: None (TTL expired)", size=11, color="mute")
    for t in range(13, 31, 2):
        f.text(x(t), 205, f"{t} s", size=11, color="mute")
    f.line(X0, 194, X1, 194, color="mute", sw=0.8)
    f.text(20, 234, "A timer fires in the first batch after its time, about 5 s after the last click. The TTL counts from the last update.", size=12, anchor="start")
    f.save("ch41-clock")


if __name__ == "__main__":
    sessions(); clock()
