"""Figures for Chapter 20. Run: python3 ch20/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit import Fig


def bench():
    # output/bench.txt
    rows = [("built-in when()", "none", 0.31), ("@udf, transpiled", "none", 0.27), ("@pandas_udf", "ArrowEvalPython", 0.70),
            ("@udf, Arrow", "ArrowEvalPython", 0.91), ("@udf, useArrow=False", "BatchEvalPython", 0.65)]
    f = Fig(780, 300, "Time of the fare-band rule in five forms")
    f.text(20, 20, "Fare band of 3,837,248 trips, noop write, median of 5 runs (bench.txt)", size=13.5, anchor="start", color="mute")
    x0, mw, mx = 200, 380, 0.91
    for i, (n, node, v) in enumerate(rows):
        y = 46 + i * 46
        f.text(x0 - 12, y + 15, n, size=13.5, anchor="end", mono=True)
        k = "ok" if node == "none" else "accent"
        f.box(x0, y, mw * v / mx, 30, kind=k, rx=4)
        f.text(x0 + mw * v / mx + 10, y + 15, f"{v:.2f} s", size=13.5, anchor="start", weight=700)
        f.text(x0 + mw * v / mx + 62, y + 15, node if node != "none" else "no Python node", size=12, anchor="start", color="mute", mono=node != "none")
    ly = 46 + 5 * 46 + 12
    f.box(x0, ly - 7, 14, 14, kind="ok", rx=2); f.text(x0 + 20, ly, "runs in the JVM", size=12.5, anchor="start")
    f.box(x0 + 150, ly - 7, 14, 14, kind="accent", rx=2); f.text(x0 + 170, ly, "rows cross to a Python worker and back", size=12.5, anchor="start")
    f.save("ch20-bench")


def batches():
    # output/arrow_batches.txt
    rows = [("defaults", 4096, 937), ("columnarReaderBatchSize=20000", 20000, 194), ("columnarInput=false", 10000, 385),
            ("columnarInput=false, maxRecords=50000", 50000, 77), ("columnarInput=false, maxRecords=0,\nmaxBytes=1 MiB", 129056, 33)]
    f = Fig(900, 320, "Size of the Arrow batches for each setting")
    f.text(20, 20, "Rows in each Arrow batch that the pandas UDF got, June trips, one double column (arrow_batches.txt)", size=13, anchor="start", color="mute")
    x0, mw, mx = 380, 320, 129056
    for i, (n, size, cnt) in enumerate(rows):
        y = 46 + i * 46
        f.text(x0 - 12, y + 15 - (8 if "\n" in n else 0), n, size=12.5, anchor="end", mono=True)
        k = "blue" if n.startswith("columnarInput=false") else "accent"
        f.box(x0, y, max(4, mw * size / mx), 30, kind=k, rx=4)
        f.text(x0 + max(4, mw * size / mx) + 10, y + 15, f"{size:,} rows, {cnt} batches", size=13, anchor="start", weight=600)
    ly = 46 + 5 * 46 + 12
    f.box(x0, ly - 7, 14, 14, kind="accent", rx=2); f.text(x0 + 20, ly, "columnar input: the Parquet batch size", size=12.5, anchor="start")
    f.box(x0, ly + 15, 14, 14, kind="blue", rx=2); f.text(x0 + 20, ly + 22, "row input: maxRecordsPerBatch, maxBytesPerBatch", size=12.5, anchor="start")
    f.save("ch20-batches")


def calls():
    # output/calls.txt: the physical plan of the filter case, 1,000 rows, 410 pass
    f = Fig(760, 400, "Two ArrowEvalPython nodes in the plan of a filter on a UDF column")
    x, w, h = 150, 360, 44
    ops = [("Project [fare_amount, big]", "", "paper", None), ("ArrowEvalPython counted(fare_amount)", "makes the big column", "accent", "410 calls"),
           ("Filter pythonUDF0", "", "paper", None), ("ArrowEvalPython counted(fare_amount)", "evaluates the filter", "accent", "1,000 calls"),
           ("FileScan parquet", "1,000 rows", "ok", None)]
    for i, (t, s, k, c) in enumerate(ops):
        y = 20 + i * 70
        f.box(x, y, w, h, t, s, kind=k, size=13, mono=True, subsize=11, submono=False)
        if i:
            f.arrow(x + w / 2, y, x + w / 2, y - 26 + 1, color="line")
        if c:
            f.text(x + w + 20, y + h / 2, c, size=16, anchor="start", weight=700, color="acc")
    f.text(x + w / 2 + 10, 20 + 2 * 70 + h + 13, "410 rows pass", size=12, anchor="start", color="mute")
    f.text(380, 375, "1,000 + 410 = 1,410 calls of the UDF for 1,000 rows", size=14.5, weight=700)
    f.save("ch20-calls")


if __name__ == "__main__":
    bench(); batches(); calls()
