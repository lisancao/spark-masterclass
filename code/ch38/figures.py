"""Figures for Chapter 38. Run: python3 ch38/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig

OUT = Path(__file__).resolve().parent / "output"


def walk(path, query="hourly"):
    """The E/S/T operators of one query in a plans-*.txt file, from the bottom to the top (glue removed)."""
    lines = (OUT / path).read_text().splitlines()
    ops, on = [], False
    for ln in lines:
        if ln.startswith(f"{query}:"):
            on = True; head = ln; continue
        if on:
            if not ln.startswith("  ") or ln.strip() == "":
                break
            tag, name = ln[2], ln[3:].strip()
            if tag in "EST":
                ops.append((tag, name))
    return head, ops[::-1]


def plans():
    cols = [("CPU, Spark 4.3.0", "plans-cpu.txt"), ("Gluten 1.7.0, 4.1.1", "plans-gluten.txt"), ("Comet 1.2.0, 4.2.0", "plans-comet.txt"),
            ("RAPIDS 26.10.0, 4.2.0", "plans-rapids.txt"), ("Auron 9.0.0, 4.2.0", "plans-auron.txt")]
    data = [(c, *walk(p)) for c, p in cols]
    n = max(len(o) for _, _, o in data)
    cw, rh = 210, 30
    f = Fig(20 + cw * 5 + 10, 110 + n * rh + 40, "Operators of the hourly query on each engine")
    f.text(20, 20, "The hourly query: operators from the scan (bottom) to the result (top), glue nodes left out (plans-*.txt)", size=13, anchor="start", color="mute")
    for j, (c, head, ops) in enumerate(data):
        x = 20 + j * cw
        f.text(x + cw / 2 - 5, 48, c, size=13, weight=700)
        f.text(x + cw / 2 - 5, 66, head.split(": ", 1)[1].replace(" operators", "").replace("1 transitions", "1 transition"), size=11, color="mute")
        base = 90 + n * rh
        for i, (tag, name) in enumerate(ops):
            k = {"E": "ok", "S": "blue", "T": "red"}[tag]
            short = name.replace("Exec", "").replace("Transformer", "T.")
            if len(short) > 26:
                short = short[:25] + "."
            f.box(x, base - (i + 1) * rh + 2, cw - 10, rh - 4, short, "", kind=k, size=11, mono=True, weight=400, rx=4)
    ly = 90 + n * rh + 22
    for x, k, t in [(20, "blue", "Spark JVM operator"), (200, "ok", "engine operator"), (360, "red", "transition between columns and rows")]:
        f.box(x, ly - 7, 14, 14, kind=k, rx=2); f.text(x + 20, ly, t, size=12, anchor="start")
    f.save("ch38-plans")


def speedups():
    # output/bench_table.txt: speed-up against the CPU on the same Spark, pass 1 / pass 2
    rows = [("comet 4.2.0", [(1.5, 1.3), (1.7, 0.9), (3.1, 1.5)]), ("rapids 4.2.0", [(1.8, 0.6), (2.1, 0.9), (2.0, 0.8)]),
            ("auron 4.2.0", [(1.3, 0.5), (3.1, 1.3), (4.5, 1.6)]), ("gluten 4.1.1", [(1.2, 0.9), (1.2, 1.0), (1.6, 1.3)])]
    qs = ["hourly", "boroughs", "routes"]
    f = Fig(860, 370, "Speed-up of each engine against the CPU, two passes")
    f.text(20, 20, "Speed-up against the CPU on the same Spark version (bench_table.txt). Each line joins pass 1 and pass 2.", size=13, anchor="start", color="mute")
    X0, X1 = 150, 820
    sx = lambda v: X0 + (v / 5.0) * (X1 - X0)
    top, rh = 70, 60
    for v in (0, 1, 2, 3, 4, 5):
        f.line(sx(v), top - 14, sx(v), top + rh * 4 - 6, color="line", sw=1.6 if v == 1 else 0.8, dash=None if v == 1 else "2 4")
        f.text(sx(v), top + rh * 4 + 8, f"{v}x", size=11.5, color="mute")
    f.text(sx(1) + 4, top - 22, "same as the CPU", size=11.5, anchor="start", color="mute")
    cols = {"hourly": "blue", "boroughs": "warn", "routes": "acc"}
    for i, (eng, vals) in enumerate(rows):
        y = top + i * rh + 14
        f.text(X0 - 14, y + 8, eng, size=13, anchor="end", mono=True)
        for k, (q, (p1, p2)) in enumerate(zip(qs, vals)):
            yy = y + (k - 1) * 13 + 8
            f.line(sx(p1), yy, sx(p2), yy, color=cols[q], sw=2)
            f.circle(sx(p1), yy, 4.5, fill="none", stroke=cols[q], sw=2)
            f.circle(sx(p2), yy, 4.5, fill=cols[q])
    ly = top + rh * 4 + 32
    x = 150
    for q in qs:
        f.circle(x, ly, 5, fill=cols[q]); f.text(x + 10, ly, q, size=12, anchor="start"); x += 100
    f.circle(x + 20, ly, 4.5, fill="none", stroke="mute", sw=2); f.text(x + 30, ly, "pass 1", size=12, anchor="start")
    f.circle(x + 110, ly, 4.5, fill="mute"); f.text(x + 120, ly, "pass 2", size=12, anchor="start")
    f.save("ch38-speedups")


def memory():
    # output/bench_table.txt: peak memory in pass 2 (MiB)
    rows = [("cpu 4.3.0", 4602, 0, 0), ("cpu 4.2.0", 3906, 0, 0), ("comet 4.2.0", 1553, 243, 0), ("rapids 4.2.0", 885, 0, 9356),
            ("auron 4.2.0", 750, 0, 0), ("cpu 4.1.1", 3217, 0, 0), ("gluten 4.1.1", 583, 440, 0)]
    f = Fig(820, 330, "Peak JVM heap of each engine")
    f.text(20, 20, "Peak memory in pass 2, MiB (bench_table.txt). The engines keep their batches outside the JVM heap.", size=13, anchor="start", color="mute")
    x0, mw, mx = 150, 400, 4602
    for i, (n, heap, off, gpu) in enumerate(rows):
        y = 44 + i * 38
        f.text(x0 - 12, y + 12, n, size=13, anchor="end", mono=True)
        k = "accent" if n.startswith("cpu") else "ok"
        f.box(x0, y, mw * heap / mx, 24, kind=k, rx=3)
        extra = []
        if off: extra.append(f"off-heap exec {off}")
        if gpu: extra.append(f"GPU pool {gpu:,}")
        f.text(x0 + mw * heap / mx + 8, y + 12, f"{heap:,}" + (" MiB heap; " + ", ".join(extra) if extra else " MiB heap"), size=12.5, anchor="start", weight=600)
    ly = 44 + 7 * 38 + 14
    f.box(x0, ly - 7, 14, 14, kind="accent", rx=2); f.text(x0 + 20, ly, "CPU baseline: JVM heap", size=12, anchor="start")
    f.box(x0 + 200, ly - 7, 14, 14, kind="ok", rx=2); f.text(x0 + 220, ly, "engine: JVM heap (other memory in the label)", size=12, anchor="start")
    f.save("ch38-memory")


if __name__ == "__main__":
    plans(); speedups(); memory()
