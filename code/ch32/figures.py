"""Figures for Chapter 32. Run: uv run python ch32/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig


def optimizer():
    # output/optimizer.txt: analyzed and optimized logical plans
    f = Fig(940, 470, "Analyzed plan and optimized plan of the join query")
    f.text(225, 20, "Analyzed plan (your query)", size=15, weight=700)
    f.text(690, 20, "Optimized plan", size=15, weight=700)
    W = 300
    def node(x, y, t, s="", kind="paper", w=W, h=44):
        f.box(x, y, w, h, t, s, kind=kind, size=13, mono=True, subsize=11)
    # left tree
    lx = 75
    node(lx, 45, "Aggregate [Zone]")
    node(lx, 105, "Project", "fare_amount * ((cast(1 as double) + 0.5) - 0.5)")
    node(lx, 165, "Filter", "Borough = Queens AND PULocationID < 140", kind="warn")
    node(lx, 225, "Join Inner", "PULocationID = LocationID")
    node(10, 300, "Relation trips", "19 columns", w=200)
    node(230, 300, "Relation zones", "4 columns", w=200)
    for y in (89, 149, 209):
        f.arrow(lx + W / 2, y + 16, lx + W / 2, y, color="line")
    f.arrow(110, 300, 180, 269, color="line"); f.arrow(330, 300, 270, 269, color="line")
    # right tree
    rx = 540
    node(rx, 45, "Aggregate [Zone]")
    node(rx, 105, "Project", "fare_amount * 1.0", kind="blue")
    node(rx, 165, "Join Inner", "PULocationID = LocationID")
    node(470, 240, "Project", "PULocationID, fare_amount", w=215, kind="ok")
    node(705, 240, "Project", "LocationID, Zone", w=215, kind="ok")
    node(470, 305, "Filter", "isnotnull(PULocationID)\nAND PULocationID < 140", w=215, h=56, kind="warn")
    node(705, 305, "Filter", "Borough = Queens\nAND LocationID < 140", w=215, h=56, kind="warn")
    node(470, 382, "Relation trips", "", w=215, h=36)
    node(705, 382, "Relation zones", "", w=215, h=36)
    for y in (89, 149):
        f.arrow(rx + W / 2, y + 16, rx + W / 2, y, color="line")
    f.arrow(577, 240, 640, 209, color="line"); f.arrow(812, 240, 750, 209, color="line")
    for x in (577, 812):
        f.arrow(x, 305, x, 284, color="line"); f.arrow(x, 382, x, 361, color="line")
    # legend of rules
    ly = 440
    items = [("warn", "PushDownPredicates, InferFiltersFromConstraints"), ("ok", "ColumnPruning"), ("blue", "ConstantFolding")]
    x = 60
    for k, t in items:
        f.box(x, ly - 9, 18, 18, kind=k, rx=3)
        f.text(x + 26, ly, t, size=12.5, anchor="start", mono=True)
        x += 26 + len(t) * 7.6 + 30
    f.save("ch32-optimizer")


def estimates():
    # output/explain_modes.txt, cost mode
    rows = [("Relation (file size)", 62.4), ("Filter", 62.4), ("Project", 6.6), ("Aggregate", 9.3)]
    f = Fig(720, 250, "Size estimates in the cost mode")
    f.text(20, 22, "explain(\"cost\"): sizeInBytes of each logical node, read from the bottom up", size=13.5, anchor="start", color="mute")
    x0, maxw = 190, 440
    for i, (n, v) in enumerate(rows):
        y = 50 + i * 46
        f.text(x0 - 12, y + 15, n, size=13.5, anchor="end", mono=True)
        kind = "warn" if n == "Aggregate" else ("ok" if n == "Project" else "paper")
        f.box(x0, y, maxw * v / 62.4, 30, kind=kind, rx=4)
        f.text(x0 + maxw * v / 62.4 + 10, y + 15, f"{v} MiB", size=13.5, anchor="start", weight=600)
    f.text(x0, 50 + 4 * 46 + 6, "The filter does not lower the estimate. The aggregate is larger than its input.", size=12.5, anchor="start", color="mute")
    f.save("ch32-estimates")


def codegen():
    # output/explain_modes.txt: the final plan after collect()
    f = Fig(760, 495, "Whole-stage codegen stages in the final plan")
    ops = [("*(2) HashAggregate", "final count, avg", "s2"), ("AQEShuffleRead coalesced", "", "aqe"),
           ("ShuffleQueryStage 0", "", "ex"), ("Exchange hashpartitioning(PULocationID, 200)", "", "ex"),
           ("*(1) HashAggregate", "partial_count, partial_avg", "s1"), ("*(1) Project", "PULocationID, fare_amount", "s1"),
           ("*(1) Filter", "pickup in June, passenger_count = 1", "s1"), ("*(1) ColumnarToRow", "batches become rows", "s1"),
           ("FileScan parquet", "4 columns, 5 pushed filters", "scan")]
    x, w, h, gap = 210, 400, 40, 12
    y = 20
    pos = []
    for t, s, k in ops:
        pos.append((y, k)); y += h + gap
    # stage regions
    s2 = [p for p in pos if p[1] == "s2"]; s1 = [p for p in pos if p[1] == "s1"]
    f.rect(x - 14, s2[0][0] - 8, w + 28, h + 16, fill="accf", stroke="acc", rx=10, dash="5 4")
    f.rect(x - 14, s1[0][0] - 8, w + 28, s1[-1][0] - s1[0][0] + h + 16, fill="accf", stroke="acc", rx=10, dash="5 4")
    f.text(x - 26, s2[0][0] + h / 2, "codegen\nstage 2", size=12.5, anchor="end", color="acc", weight=600)
    f.text(x - 26, (s1[0][0] + s1[-1][0] + h) / 2 - 8, "codegen stage 1:\none generated\nJava loop", size=12.5, anchor="end", color="acc", weight=600)
    ex = [p for p in pos if p[1] == "ex"]
    f.line(40, ex[1][0] + h + gap / 2, 720, ex[1][0] + h + gap / 2, color="mute", dash="3 4")
    f.text(x + w + 26, ex[0][0] + h, "shuffle files:\nnot in any\ncodegen stage", size=12.5, anchor="start", color="mute")
    for (t, s, k), (yy, _) in zip(ops, pos):
        kind = {"s1": "paper", "s2": "paper", "aqe": "blue", "ex": "ghost", "scan": "ok"}[k]
        f.box(x, yy, w, h, t, s, kind=kind, size=13, mono=True, subsize=11, submono=False)
    for i in range(len(pos) - 1):
        f.arrow(x + w / 2, pos[i + 1][0], x + w / 2, pos[i][0] + h + 1, color="line")
    f.save("ch32-codegen")


if __name__ == "__main__":
    optimizer(); estimates(); codegen()
