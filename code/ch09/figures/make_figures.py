"""Figures for Chapter 9. Numbers from code/ch09/output/plans.txt, type_system.txt and drift.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. build + analyze + optimize time (plans.txt, median of 7 after a warmup, ms)
cols = [25, 50, 100, 200, 400]
loop = [48.9, 101.1, 286.2, 957.4, 7582.4]
once = [25.3, 47.4, 110.7, 285.9, 703.5]
f = Fig(760, 330, "Driver time to build a DataFrame with n new columns")
X0, Y0, PW, PH = 80, 30, 560, 240
def px(c): return X0 + PW * c / 400
def py(v): return Y0 + PH - PH * v / 8000
for v in range(0, 8001, 2000):
    f.line(X0, py(v), X0 + PW, py(v), c="grid", sw=1)
    f.text(X0 - 8, py(v) + 4, f"{v:,}", 11, 400, anchor="end", c="mute")
for c in cols:
    f.text(px(c), Y0 + PH + 18, str(c), 11, 400, c="mute")
f.text(X0 + PW / 2, Y0 + PH + 38, "new columns", 12, 500, c="mute")
f.text(20, Y0 + PH / 2, "ms", 12, 500, c="mute")
for series, k, lab in ((loop, "acc", "withColumn loop"), (once, "blue", "withColumns")):
    pts = [(round(px(c), 1), round(py(v), 1)) for c, v in zip(cols, series)]
    f.path(pts, c=k, sw=2.5, head=False)
    for (x, y) in pts:
        f.circle(x, y, 4, kind=k)
f.text(px(400) + 8, py(7582.4) + 4, "7,582.4 ms", 12, 700, anchor="start", c="acc")
f.text(px(400) + 8, py(703.5) + 4, "703.5 ms", 12, 700, anchor="start", c="blue")
f.text(px(200) - 6, py(957.4) - 10, "957.4", 11, 600, anchor="end", c="acc")
f.text(px(200) + 4, py(285.9) + 16, "285.9", 11, 600, anchor="start", c="blue")
f.rect(X0 + 20, Y0 + 10, 14, 4, "acc", r=1); f.text(X0 + 40, Y0 + 16, "withColumn in a loop", 12, 600, anchor="start", c="acc")
f.rect(X0 + 20, Y0 + 30, 14, 4, "blue", r=1); f.text(X0 + 40, Y0 + 36, "one withColumns call", 12, 600, anchor="start", c="blue")
f.text(X0 + 20, Y0 + 58, "median of 7 runs after a warmup, local[*], 16 cores", 11, 400, anchor="start", c="mute", italic=True)
f.save("ch09-chain-cost")

# 2. TIMESTAMP vs TIMESTAMP_NTZ (type_system.txt section 3)
f = Fig(800, 250, "The same text under two session time zones")
f.text(20, 26, "'2026-06-01 10:00:00' as seconds since 1970 UTC", 14, 700, anchor="start")
X0, X1 = 80, 740
t0, t1 = 1780300800, 1780329600  # 2026-06-01 08:00 .. 16:00 UTC
def tx(e): return X0 + (X1 - X0) * (e - t0) / (t1 - t0)
y = 190
f.line(X0, y, X1, y, c="line", sw=1.5)
for h in range(8, 17, 2):
    e = t0 + (h - 8) * 3600
    f.line(tx(e), y - 4, tx(e), y + 4, c="line")
    f.text(tx(e), y + 20, f"{h:02d}:00 UTC", 11, 400, c="mute")
for e, lab, sub, k, yy in ((1780308000, "TIMESTAMP, session UTC", "1780308000", "blue", 60),
                           (1780322400, "TIMESTAMP, session America/New_York", "1780322400", "acc", 60),
                           (1780308000, "TIMESTAMP_NTZ, both sessions", "1780308000", "ok", 120)):
    f.line(tx(e), yy + 30, tx(e), y, c=k, sw=1.5, dash=True)
    f.box(tx(e) - 120, yy, 240, 40, lab, sub, kind=k, size=12, subsize=11, sub_mono=True)
    f.circle(tx(e), y, 5, kind=k)
f.arrow(tx(1780308000) + 6, y - 14, tx(1780322400) - 6, y - 14, "14,400 s = 4 hours", c="acc", lc="acc", both=True)
f.save("ch09-timezones")

# 3. Three days of events read through `event STRING, amount DOUBLE` (drift.txt sections 1 and 3)
f = Fig(800, 260, "Three days of events through one schema")
days = [("events_2026-10-01.json", "amount: double", 20, 20, "ok"),
        ("events_2026-10-02.json", "amount: string", 20, 15, "acc"),
        ("events_2026-10-03.json", "amount: struct<currency, value>", 21, 0, "bad")]
f.text(20, 26, "Inferred type of amount, and the rows that fit amount DOUBLE", 14, 700, anchor="start")
for i, (fn, ty, rows, ok, k) in enumerate(days):
    x = 20 + i * 260
    f.box(x, 44, 240, 56, fn, ty, kind=k, size=12, mono=True, subsize=12, sub_mono=True)
    f.arrow(x + 120, 102, x + 120, 128)
    f.text(x, 150, f"rows {rows}", 12, 500, anchor="start", c="mute")
    for r in range(rows):
        cx = x + 8 + (r % 11) * 21
        cy = 168 + (r // 11) * 22
        f.rect(cx - 7, cy - 7, 16, 16, "ok" if r < ok else "bad", r=3, fill=None if r < ok else "none")
    f.text(x + 240, 150, f"amount_ok {ok}", 12, 700, anchor="end", c="ok" if ok == rows else ("acc" if ok else "bad"))
f.rect(20, 236, 12, 12, "ok", r=2); f.text(38, 246, "value fits", 11, 500, anchor="start", c="mute")
f.rect(120, 236, 12, 12, "bad", r=2, fill="none"); f.text(138, 246, "NULL (did not fit)", 11, 500, anchor="start", c="mute")
f.save("ch09-drift-days")
