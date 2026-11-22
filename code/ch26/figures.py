"""Figures for Chapter 26. Run: python3 ch26/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig


def routing():
    # output/m2_parse.txt (46 lines: 42 good, 4 rejects) and m3_dead_letters.txt (silver 38 distinct ids)
    f = Fig(880, 290, "Where the 46 lines of the four drops go")
    f.text(20, 20, "The four drops through parse_events and route_batch (m2_parse.txt, m3_dead_letters.txt)", size=13, anchor="start", color="mute")
    f.box(20, 110, 150, 80, "46 lines", "text source,\nvalue STRING", kind="paper", size=16)
    f.box(240, 70, 170, 70, "42 good events", "error IS NULL", kind="blue", size=15)
    f.box(240, 190, 170, 70, "4 dead letters", "raw line + reason", kind="red", size=15)
    f.arrow(170, 140, 238, 108, color="mute"); f.arrow(170, 160, 238, 222, color="mute")
    f.box(540, 70, 170, 70, "38 in silver", "38 distinct event_id", kind="ok", size=15)
    f.arrow(410, 105, 538, 105, color="mute")
    f.text(474, 124, "dedup: 4 copies", size=11.5, color="mute")
    f.text(474, 139, "removed", size=11.5, color="mute")
    reasons = ["bad_json", "bad_ts", "missing_user", "future_ts"]
    for i, r in enumerate(reasons):
        f.box(540 + (i % 2) * 165, 180 + (i // 2) * 44, 155, 36, r, "", kind="red", size=13, mono=True, weight=400)
    f.arrow(410, 225, 538, 210, color="mute")
    f.box(740, 70, 130, 70, "drift alarm", "[app_version] x 20", kind="warn", size=13)
    f.path("M325,70 L325,50 L805,50 L805,68", color="warn", sw=1.4, dash="4 3", arrow=True)
    f.text(550, 42, "unknown_keys", size=11.5, color="warn", mono=True)
    f.save("ch26-routing")


def statebounds():
    # output/m5_state_bounds.txt part 1
    a = [1000, 2000, 3000, 4000, 5000, 6000]
    b = [1000, 2000, 3000, 3001, 3000, 3000]
    f = Fig(820, 300, "State rows of the two deduplication operators")
    f.text(20, 20, "State rows after each batch of 1,000 events, 10 minutes of event time per batch (m5_state_bounds.txt)", size=12.5, anchor="start", color="mute")
    X0, bw, base, H = 80, 100, 250, 190
    for i in range(6):
        x = X0 + i * bw
        ha, hb = H * a[i] / 6000, H * b[i] / 6000
        f.box(x + 8, base - ha, 38, ha, kind="red", rx=3)
        f.box(x + 50, base - hb, 38, hb, kind="ok", rx=3)
        f.text(x + 27, base - ha - 9, f"{a[i]:,}", size=11, weight=600)
        f.text(x + 69, base - hb - 9, f"{b[i]:,}", size=11, weight=600)
        f.text(x + 48, base + 14, f"batch {i}", size=11.5, color="mute")
    f.line(X0, base, X0 + 6 * bw, base, color="mute")
    f.box(X0, 278, 14, 14, kind="red", rx=2); f.text(X0 + 20, 285, "dropDuplicates", size=12, anchor="start", mono=True)
    f.box(X0 + 200, 278, 14, 14, kind="ok", rx=2); f.text(X0 + 220, 285, "dropDuplicatesWithinWatermark", size=12, anchor="start", mono=True)
    f.text(X0 + 6 * bw + 10, 60, "both kept\n6,000 unique\nevents", size=12, anchor="start", color="mute")
    f.save("ch26-statebounds")


def policy():
    # output/m5_state_bounds.txt part 2
    mn = ["00:00", "08:34", "08:44", "08:54", "09:04", "09:14", "09:24"]
    mx = ["00:00", "09:04", "09:14", "09:24", "09:34", "09:44", "09:54"]
    m = lambda s: int(s[:2]) * 60 + int(s[3:])
    f = Fig(780, 290, "Global watermark per batch with the min and max policies")
    f.text(20, 20, "Two inputs into one union, the phone input 30 minutes behind the web input (m5_state_bounds.txt)", size=12.5, anchor="start", color="mute")
    X0, X1, Y0, Y1 = 90, 600, 240, 50
    T0, T1 = m("08:30"), m("10:00")
    y = lambda t: Y0 - (t - T0) / (T1 - T0) * (Y0 - Y1)
    x = lambda i: X0 + (i - 1) / 5 * (X1 - X0)
    for t in range(T0, T1 + 1, 30):
        f.line(X0, y(t), X1, y(t), color="line", sw=0.8, dash="2 4")
        f.text(X0 - 10, y(t), f"{t // 60:02d}:{t % 60:02d}", size=11.5, anchor="end", color="mute")
    for i in range(1, 7):
        f.text(x(i), Y0 + 16, f"batch {i}", size=11, color="mute")
    for vals, col, name, res in [(mn, "ok", "min (default)", "2,400 of 2,400 counted"), (mx, "red", "max", "1,600 of 2,400 counted")]:
        d = "M" + " L".join(f"{x(i):.1f},{y(m(v)):.1f}" for i, v in enumerate(vals) if i)
        f.path(d, color=col, sw=2.4)
        for i, v in enumerate(vals):
            if i:
                f.circle(x(i), y(m(v)), 4, fill=col)
        f.text(X1 + 14, y(m(vals[-1])) - 8, name, size=13, anchor="start", color=col, weight=700)
        f.text(X1 + 14, y(m(vals[-1])) + 9, res, size=12, anchor="start", color="fg")
    f.text(X0, 274, "Batch 0 has no watermark (00:00). The time axis is the global watermark.", size=11.5, anchor="start", color="mute")
    f.save("ch26-policy")


if __name__ == "__main__":
    routing(); statebounds(); policy()
