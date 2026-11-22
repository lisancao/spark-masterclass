"""Chapter 33 figures. Numbers and ids from code/data/retail/orders and code/ch33/output/."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def routing():
    # run 1 input, in file order (orders_2026-10-01.json, orders_2026-10-02.json)
    rows = [("o-1001", "silver"), ("o-1002", "silver"), ("o-1003", "silver"), ("o-1004", "silver"),
            ("o-1002", "copy"), ("o-1005", "invalid_price"), ("o-1006", "silver"), ("o-1007", "silver"),
            ("o-1008", "invalid_quantity"), ("o-1009", "unknown_customer"), ("o-1010", "silver")]
    f = Fig(900, 470, "Run 1: where each bronze row goes")
    f.text(20, 28, "bronze_orders (run 1): 11 rows", size=15, weight=700, c="br")
    f.text(470, 28, "silver_orders: 7 rows", size=15, weight=700, c="si")
    f.text(470, 268, "quarantine_orders: 3 rows", size=15, weight=700, c="bad")
    f.text(470, 410, "not kept: a second copy", size=15, weight=700, c="mute")
    ys, yq = 44, 284
    for i, (oid, dest) in enumerate(rows):
        y = 44 + i * 36
        c = {"silver": "si", "copy": "gr"}.get(dest, "bad")
        f.rect(20, y, 150, 28, c="br", fill=0.14, rx=5)
        f.text(32, y + 19, oid, size=13, mono=True)
        if dest == "silver":
            ty = ys; ys += 30
            f.rect(470, ty, 150, 26, c="si", fill=0.16, rx=5)
            f.text(482, ty + 18, oid, size=13, mono=True)
        elif dest == "copy":
            ty = 422
            f.rect(470, ty, 150, 26, c="gr", fill=0.08, rx=5, dash="4 3")
            f.text(482, ty + 18, oid, size=13, mono=True, c="mute")
            f.text(632, ty + 18, "row_number() > 1: the first copy wins", size=12, c="mute")
        else:
            ty = yq; yq += 30
            f.rect(470, ty, 150, 26, c="bad", fill=0.14, rx=5)
            f.text(482, ty + 18, oid, size=13, mono=True)
            f.text(632, ty + 18, f"_reason = {dest}", size=12, mono=True, c="bad")
        x1, y1, x2, y2 = 170, y + 14, 466, ty + 13
        f.path(f"M{x1},{y1} C{x1+150},{y1} {x2-150},{y2} {x2},{y2}", c=c, w=1.3, arrow=True)
    f.text(632, 66, "o-1010 is cancelled: silver keeps it,", size=12, c="mute")
    f.text(632, 82, "billable_orders and gold do not", size=12, c="mute")
    f.text(632, 128, "check_orders_accounted_for:", size=12, c="ok", weight=600)
    f.text(632, 144, "10 unique in bronze = 7 + 3", size=12, c="ok", mono=True)
    return f.save("ch33-routing", CHECK)


def gate():
    f = Fig(900, 330, "Run 2: the gate fails, and silver_orders is empty")
    # time axis
    X0, X1 = 200, 880
    labels = [("end of run 1", X0), ("run 2 starts\nTRUNCATE", 310), ("10:30:43\nattempt 1 FAILED", 450),
              ("10:30:49\nattempt 2 FAILED", 590), ("10:31:00\nattempt 3 FAILED", 720), ("10:31:01\ngold SKIPPED", 840)]
    f.line(X0 - 10, 250, X1, 250, c="line", arrow=True)
    for lab, x in labels:
        f.line(x, 244, x, 256, c="line")
        for i, part in enumerate(lab.split("\n")):
            f.text(x, 272 + i * 15, part, size=12, anchor="middle", c="mute" if i == 0 else "ink",
                   mono=(i == 0 and part[:2].isdigit()))
    # lanes
    lanes = [("quality_report", 60), ("silver_orders", 120), ("revenue_by_currency", 180)]
    for name, y in lanes:
        f.text(20, y + 22, name, size=13, mono=True, weight=600)
    # quality report: day 3 ratio
    f.rect(310, 60, 150, 34, c="ok", fill=0.18)
    f.text(385, 82, "day 3: ratio 0.75", size=12, anchor="middle")
    # silver: 103 rows bar then 0
    f.rect(X0, 120, 110, 34, c="si", fill=0.3)
    f.text(X0 + 55, 142, "103 rows", size=13, anchor="middle", weight=600)
    f.rect(310, 120, 570, 34, c="si", fill=0.0, dash="5 4")
    f.text(325, 142, "0 rows", size=13, weight=600, c="bad")
    for x in (450, 590, 720):
        f.circle(x, 137, 9, c="bad")
        f.text(x, 141, "!", size=12, anchor="middle", weight=700, c="soft")
    f.text(450, 108, "assert_true: more than 20% bad rows (USER_RAISED_EXCEPTION)", size=12, c="bad")
    # gold
    f.rect(X0, 180, 110, 34, c="go", fill=0.3)
    f.text(X0 + 55, 202, "2 rows", size=13, anchor="middle", weight=600)
    f.rect(310, 180, 570, 34, c="go", fill=0.0, dash="5 4")
    f.text(325, 202, "0 rows: truncated, then SKIPPED", size=13, weight=600, c="bad")
    f.text(20, 316, "Iceberg keeps the history: snapshot append (+103, total 103), then delete (-103, total 0).",
           size=12, c="mute")
    return f.save("ch33-gate-timeline", CHECK)


def rules():
    data = [("customer_known", 4), ("quantity_positive", 4), ("unit_price_is_number", 3),
            ("customer_id_present", 2), ("quantity_is_integer", 2), ("currency_allowed", 1),
            ("order_ts_not_after_file_day", 1), ("status_allowed", 1)]
    f = Fig(640, 290, "Run 1: rows in quarantine for each broken rule")
    f.text(20, 26, "Broken rules in the quarantine after run 1 (120 rows, 14 quarantined)", size=14, weight=600)
    for i, (r, n) in enumerate(data):
        y = 48 + i * 28
        f.text(250, y + 15, r, size=12, mono=True, anchor="end")
        f.rect(260, y, n * 80, 20, c="bad", fill=0.35, rx=3, stroke=0)
        f.text(266 + n * 80, y + 15, str(n), size=12, weight=600)
    f.note("a row can break more than one rule", y=284)
    return f.save("ch33-rules", CHECK)


if __name__ == "__main__":
    for fn in (routing, gate, rules):
        print(fn())
