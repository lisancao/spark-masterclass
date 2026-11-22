"""Chapter 51 figures. Events from code/ch51 change files, tables from code/ch51/output/drops.txt."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def three_ways():
    f = Fig(900, 330, "Three ways to use the events of c-01")
    ev = [("file 1", "Toronto", 1), ("file 1", "Hanoi", 5), ("file 2", "Ottawa", 4)]
    f.text(20, 26, "The events for c-01, in the order of arrival", size=14, weight=700)
    for i, (fl, city, seq) in enumerate(ev):
        x = 20 + i * 180
        f.rect(x, 40, 165, 44, c="br", fill=0.14)
        f.text(x + 10, 58, f"{fl}", size=11, c="mute")
        f.text(x + 10, 76, f"{city}, seq {seq}", size=13, mono=True)
        if i < 2:
            f.line(x + 165, 62, x + 178, 62, arrow=True)
    panels = [("Append each event", "bad", ["Toronto  seq 1", "Hanoi    seq 5", "Ottawa   seq 4"],
               "3 rows for one customer: wrong"),
              ("Apply in arrival order", "bad", ["Ottawa   seq 4"], "the late event wins: wrong"),
              ("Apply by key and seq", "ok", ["Hanoi    seq 5"], "the highest seq wins: correct")]
    for i, (t, c, rows, verdict) in enumerate(panels):
        x = 20 + i * 295
        f.rect(x, 120, 275, 190, c=c, fill=0.07)
        f.text(x + 14, 146, t, size=14, weight=700, c=c)
        f.text(x + 14, 170, "c-01 in the target:", size=12, c="mute")
        for j, r in enumerate(rows):
            f.rect(x + 14, 180 + j * 30, 247, 24, c="gr", fill=0.12, rx=4, stroke=1)
            f.text(x + 24, 197 + j * 30, r, size=13, mono=True)
        f.text(x + 14, 296, verdict, size=12, c=c, weight=600)
    return f.save("ch51-three-ways", CHECK)


def history():
    # customers_history and customers_city_history after drop 3 (drops.txt)
    full = [("c-01", [(1, 4, "Toronto"), (4, 5, "Ottawa"), (5, None, "Hanoi")], None),
            ("c-02", [(2, 6, "Vancouver")], 6),
            ("c-03", [(3, 9, "Chloe Martin"), (9, None, "Chloe Martin-Roy")], None),
            ("c-04", [(7, 10, "Toronto"), (10, None, "Hanoi")], None)]
    city_c03 = [(3, None, "Chloe Martin-Roy, Montreal")]
    X0, U = 110, 60   # x of seq 0, px per seq unit
    f = Fig(900, 400, "customers_history after three files, by sequence number")
    f.text(20, 26, "customers_history (all columns tracked), after three files", size=14, weight=700)
    f.text(880, 26, "dashed: the current version (__END_AT is NULL)", size=12, c="mute", anchor="end")
    def axis(y):
        f.line(X0, y, X0 + 12.4 * U, y, c="line", arrow=True)
        for s in range(1, 13):
            f.line(X0 + s * U, y - 4, X0 + s * U, y + 4, c="line")
            f.text(X0 + s * U, y + 18, str(s), size=11, anchor="middle", c="mute")
        f.text(X0 + 12.6 * U, y + 4, "seq", size=11, c="mute")
    cols = ["si", "go", "ok"]
    for r, (cid, vers, deleted) in enumerate(full):
        y = 44 + r * 46
        f.text(20, y + 22, cid, size=13, mono=True, weight=600)
        for k, (a, b, lab) in enumerate(vers):
            x1 = X0 + a * U
            x2 = X0 + (b if b else 12.2) * U
            f.rect(x1, y + 4, x2 - x1 - 2, 28, c=cols[k % 3], fill=0.22, rx=4,
                   dash=None if b else "5 3")
            f.text(x1 + 6, y + 23, lab, size=12)
        if deleted:
            x = X0 + deleted * U
            f.text(x + 8, y + 23, "DELETE at seq 6: no version follows", size=12, c="bad")
    axis(234)
    f.text(20, 290, "customers_city_history (TRACK HISTORY ON (city)): c-03 only", size=14, weight=700)
    y = 302
    f.text(20, y + 22, "c-03", size=13, mono=True, weight=600)
    for a, b, lab in city_c03:
        x1 = X0 + a * U
        x2 = X0 + 12.2 * U
        f.rect(x1, y + 4, x2 - x1 - 2, 28, c="si", fill=0.22, rx=4, dash="5 3")
        f.text(x1 + 6, y + 23, lab + ": the name change at seq 9 starts no version", size=12)
    axis(356)
    return f.save("ch51-history", CHECK)


def aux():
    f = Fig(900, 250, "Each Auto CDC target has an auxiliary state table")
    f.box(20, 30, 250, 70, "customer_changes", "bronze streaming table", c="br", mono=True)
    f.box(330, 20, 260, 56, "customers_current", "SCD Type 1 target", c="si", mono=True)
    f.box(330, 140, 260, 56, "customers_history", "SCD Type 2 target", c="si", mono=True)
    f.line(270, 60, 326, 48, arrow=True)
    f.line(270, 72, 326, 166, arrow=True)
    f.box(630, 20, 255, 56, "__spark_autocdc_aux_state_", "customers_current", c="gr", mono=True, size=12)
    f.box(630, 140, 255, 56, "__spark_autocdc_aux_state_", "customers_history", c="gr", mono=True, size=12)
    f.line(590, 48, 626, 48, c="gr", dash="4 3")
    f.line(590, 168, 626, 168, c="gr", dash="4 3")
    f.text(640, 98, "tombstone: c-02, delete at seq 6", size=12, mono=True, c="mute")
    f.text(640, 218, "hidden event: c-03 Montreal, seq 3", size=12, mono=True, c="mute")
    f.text(20, 140, "Each run reads the auxiliary", size=12, c="mute")
    f.text(20, 156, "table to put a late event", size=12, c="mute")
    f.text(20, 172, "into its correct position.", size=12, c="mute")
    f.text(20, 236, "Values from internals.py after three files. customers_city_history has its own auxiliary table too.",
           size=12, c="mute")
    return f.save("ch51-aux", CHECK)


if __name__ == "__main__":
    for fn in (three_ways, history, aux):
        print(fn())
