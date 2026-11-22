"""Chapter 41 figures. Numbers from code/ch41/output/sketches.txt."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def daily():
    days = [9000, 12140, 13207, 13675, 13892, 13768, 13805]
    f = Fig(900, 300, "Daily distinct users, their sum and the distinct users of the week")
    f.text(20, 26, "Distinct users: seven days against the week (one scale)", size=14, weight=700)
    BASE, S = 260, 200 / 89494
    x = 40
    for i, n in enumerate(days):
        f.rect(x, BASE - n * S, 50, n * S, c="si", fill=0.4, rx=2, stroke=0)
        f.text(x + 25, BASE - n * S - 6, f"{n:,}", size=11, anchor="middle", mono=True)
        f.text(x + 25, BASE + 16, f"day {i}", size=11, anchor="middle", c="mute")
        x += 62
    f.line(30, BASE, 860, BASE, c="line")
    for xx, n, c, lab in ((540, 89494, "bad", "sum of the 7 days"), (700, 33233, "ok", "distinct in the week")):
        f.rect(xx, BASE - n * S, 90, n * S, c=c, fill=0.4, rx=2, stroke=0)
        f.text(xx + 45, BASE - n * S - 6, f"{n:,}", size=12, anchor="middle", mono=True, c=c, weight=700)
        f.text(xx + 45, BASE + 16, lab, size=11, anchor="middle", c="mute")
    f.text(20, 292, "A user on three days counts three times in the sum. Exact counts from plain Python over the same events.",
           size=11, c="mute")
    return f.save("ch41-daily", CHECK)


def table():
    f = Fig(900, 300, "A table of sketches: one row for each day and country")
    f.text(20, 26, "sketch table: 35 rows (7 days x 5 countries), 0.88 MB; the raw JSON was 15.5 MB", size=14, weight=700)
    cs = ["CA", "US", "VN", "FR", "BR"]
    X0, Y0, W, H = 70, 60, 52, 34
    for j, c in enumerate(cs):
        f.text(X0 - 10, Y0 + j * H + 22, c, size=12, anchor="end", mono=True)
    for d in range(7):
        f.text(X0 + d * W + W / 2, Y0 - 8, f"day {d}", size=11, anchor="middle", c="mute")
        for j in range(5):
            hi = (d in (0, 1))
            f.rect(X0 + d * W + 2, Y0 + j * H + 2, W - 4, H - 4, c="go" if hi else "si", fill=0.35 if hi else 0.14, rx=3, stroke=0)
    f.text(X0, Y0 + 5 * H + 22, "each cell: users_hll, users_theta, latency_kll (BINARY) and events (BIGINT)", size=11, c="mute")
    qx = 470
    f.text(qx, 70, "Answers from the 35 rows, raw events deleted:", size=13, weight=600)
    lines = [("hll_union_agg, all rows", "week: 32,950 (exact 33,233)", "si"),
             ("hll_union_agg, group by country", "VN: 6,611 (exact 6,707)", "si"),
             ("theta_intersection, day 0 and day 1", "5,229 (exact 5,175)", "go"),
             ("kll_merge_agg_double, all rows", "p50: 54.58 ms (exact 54.46)", "si"),
             ("sum(events), all rows", "270,453: exact, counts add", "ok")]
    for i, (q, a, c) in enumerate(lines):
        y = 100 + i * 36
        f.text(qx, y, q, size=12, mono=True, c=c)
        f.text(qx, y + 16, a, size=12)
    return f.save("ch41-table", CHECK)


def kll():
    rows = [(0.5, 54.58, 54.46, 0.5009), (0.9, 153.88, 152.40, 0.9021), (0.99, 334.48, 352.74, 0.9881),
            (0.999, 790.66, 655.78, 0.9995)]
    f = Fig(900, 260, "KLL keeps the rank close, and the value can still be far in the tail")
    f.text(20, 26, "Weekly latency from 35 merged KLL sketches: estimate against exact value", size=14, weight=700)
    X0, S = 440, 0.55   # px per ms
    f.line(X0, 230, X0 + 800 * S, 230, c="line")
    for v in (0, 200, 400, 600, 800):
        f.text(X0 + v * S, 246, f"{v} ms", size=10, anchor="middle", c="mute")
    for i, (r, est, ex, rk) in enumerate(rows):
        y = 50 + i * 44
        f.text(20, y + 16, f"rank {r}", size=13, mono=True, weight=600)
        f.text(120, y + 16, f"estimate {est}, exact {ex}", size=11, mono=True)
        f.text(120, y + 31, f"true rank of the estimate: {rk}", size=10, c="mute")
        lo, hi = min(est, ex), max(est, ex)
        f.line(X0 + lo * S, y + 12, X0 + hi * S, y + 12, c="bad" if hi - lo > 50 else "line", w=3)
        f.circle(X0 + ex * S, y + 12, 6, c="ok")
        f.circle(X0 + est * S, y + 12, 6, c="go")
    f.circle(120, 236, 5, c="ok"); f.text(130, 240, "exact value", size=11)
    f.circle(220, 236, 5, c="go"); f.text(230, 240, "estimate", size=11)
    return f.save("ch41-kll", CHECK)


if __name__ == "__main__":
    for fn in (daily, table, kll):
        print(fn())
