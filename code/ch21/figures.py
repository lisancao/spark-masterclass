"""Figures for Chapter 21. Run: python3 ch21/figures.py (from code/). Numbers from output/*.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "common"))
from figkit2 import Fig


def arrow_decision():
    # udtf_core.py order of definitions; plan nodes from output/udtf_core.txt
    f = Fig(860, 340, "Arrow or not: decided when udtf() runs")
    f.text(20, 20, "udtf_core.py, from the top of the file down. The setting pythonUDTF.arrow.enabled is true.", size=13, anchor="start", color="mute")
    code = [("@udtf(returnType=\"n int\")\nclass BeforeSession: ...", "no active session: no setting to read", "warn"),
            ("spark = session(\"ch21-udtf\", ...)", "the session starts here", "paper"),
            ("@udtf(returnType=\"n int\")\nclass AfterSession: ...", "reads the setting from the session", "ok")]
    nodes = ["BatchEvalPythonUDTF", None, "ArrowEvalPythonUDTF, eval type 301"]
    y = 44
    for (c, why, k), node in zip(code, nodes):
        h = 62 if "\n" in c else 44
        f.box(20, y, 360, h, c, "", kind=k, size=13, mono=True, weight=400)
        f.text(24, y + h + 12, why, size=12, anchor="start", color="mute", italic=True)
        if node:
            f.arrow(392, y + h / 2, 470, y + h / 2, color="mute")
            f.box(478, y + h / 2 - 17, 362, 34, node, "", kind="red" if node.startswith("Batch") else "ok", size=13, mono=True)
        y += h + 36
    f.text(20, y + 8, "A UDTF that a module defines before the session starts gets the form without Arrow, with no warning.", size=12.5, anchor="start")
    f.save("ch21-arrow-decision")


def jsontime():
    # output/json_table.txt
    rows = [("Python UDTF", 2.47, "ArrowEvalPythonUDTF", "accent"), ("JSON_TABLE", 1.31, "Generate json_table", "blue"),
            ("from_json + posexplode", 0.20, "Generate posexplode", "ok")]
    f = Fig(780, 210, "Time to turn 200,000 JSON documents into item rows")
    f.text(20, 20, "200,000 documents to 698,132 item rows, noop write, median of 5 runs (json_table.txt)", size=13, anchor="start", color="mute")
    x0, mw = 220, 400
    for i, (n, v, node, k) in enumerate(rows):
        y = 44 + i * 50
        f.text(x0 - 12, y + 16, n, size=13.5, anchor="end", mono=True)
        f.box(x0, y, max(6, mw * v / 2.47), 32, kind=k, rx=4)
        f.text(x0 + max(6, mw * v / 2.47) + 10, y + 9, f"{v:.2f} s", size=14, anchor="start", weight=700)
        f.text(x0 + max(6, mw * v / 2.47) + 10, y + 25, node, size=11.5, anchor="start", color="mute", mono=True)
    f.text(20, 200, "All three give the same rows and the same sum(qty*price): 84,944,653.68.", size=12.5, anchor="start")
    f.save("ch21-jsontime")


if __name__ == "__main__":
    arrow_decision(); jsontime()
