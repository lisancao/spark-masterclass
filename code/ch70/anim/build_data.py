"""Write passes/data.js from code/ch70/output/iterative.txt, so the animation shows measured seconds only."""
import json
import re
from pathlib import Path

here = Path(__file__).resolve().parent
rows = []
for line in (here.parent / "output" / "iterative.txt").read_text().splitlines():
    m = re.match(r"\s+(\d+)\s+([\d.]+)\s+[\d,]+\s+([\d.]+)\s+[\d,]+$", line)
    if m:
        rows.append({"pass": int(m[1]), "mr": float(m[2]), "rdd": float(m[3])})
med = re.search(r"mapreduce ([\d.]+) s, rdd ([\d.]+) s \(([\d.]+)x\)", (here.parent / "output" / "iterative.txt").read_text())
data = {"source": "code/ch70/output/iterative.txt", "passes": rows,
        "median_mr": float(med[1]), "median_rdd": float(med[2]), "ratio": float(med[3])}
(here / "passes" / "data.js").write_text("window.DATA = " + json.dumps(data) + ";\n")
(here / "passes" / "data.json").write_text(json.dumps(data, indent=1) + "\n")
print(data["ratio"], sum(r["mr"] for r in rows), sum(r["rdd"] for r in rows))
