"""Chapter 76: data for the bounding-box animation and the map figure, from the saved Parquet outputs.

Writes anim/bbox-prefilter/data.json (real zones and stations in a window of lower Manhattan,
and the pair counts and timings from output/pip.txt and output/pip_bench.txt).
Needs zones.py, pip.py and pip_bench.py first.  Run from code/:  uv run python ch76/anim_data.py
"""
import json
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow.parquet as pq
from geo76 import OUT
from common.book import work

W = work("ch76", clean=False)
HERE = Path(__file__).resolve().parent
X0, X1, Y0, Y1 = 983000.0, 993000.0, 199500.0, 207000.0      # a window of lower Manhattan, US feet
CELL = 5280.0

zones = pq.read_table(W / "zones.parquet", columns=["location_id", "zone", "rings", "xmin", "xmax", "ymin", "ymax"]).to_pylist()
pts = {p["pid"]: p for p in pq.read_table(W / "points.parquet", columns=["pid", "x", "y", "trips"]).to_pylist()}
hit = {h["pid"]: h["location_id"] for h in pq.read_table(W / "pip.parquet", columns=["pid", "location_id"]).to_pylist()}


def keep(z):
    return z["xmax"] > X0 and z["xmin"] < X1 and z["ymax"] > Y0 and z["ymin"] < Y1


def thin(ring, step=40.0):
    out = [ring[0]]
    for p in ring[1:]:
        if abs(p["x"] - out[-1]["x"]) + abs(p["y"] - out[-1]["y"]) > step:
            out.append(p)
    return out + [ring[-1]]


def fx(x):
    return round((x - X0) / (X1 - X0), 4)


def fy(y):
    return round(1 - (y - Y0) / (Y1 - Y0), 4)       # SVG y grows downward


sel = [z for z in zones if keep(z)]
data = {
    "window": [X0, X1, Y0, Y1], "cell_ft": CELL,
    "grid_x": [fx(k * CELL) for k in range(int(X0 // CELL), int(X1 // CELL) + 2)],
    "grid_y": [fy(k * CELL) for k in range(int(Y0 // CELL), int(Y1 // CELL) + 2)],
    "zones": [{"id": z["location_id"], "name": z["zone"],
               "rings": [[[fx(p["x"]), fy(p["y"])] for p in thin(r)] for r in z["rings"]],
               "bbox": [fx(z["xmin"]), fy(z["ymax"]), fx(z["xmax"]), fy(z["ymin"])]} for z in sel],
    "points": [{"x": fx(p["x"]), "y": fy(p["y"]), "zone": hit.get(pid)} for pid, p in pts.items()
               if X0 < p["x"] < X1 and Y0 < p["y"] < Y1],
}
txt = (OUT / "pip.txt").read_text()
nums = [int(n.replace(",", "")) for n in re.findall(r"^\s+(?:all pairs|same grid cell|same cell and inside bbox)\s+([\d,]+)", txt, re.M)]
bench = (OUT / "pip_bench.txt").read_text()
med = {m.group(1).strip(): float(m.group(2)) for m in re.finditer(r"^\s+(distinct points, [^\d]+?)\s+[\d,]+\s+([\d.]+)", bench, re.M)}
data["pairs"] = {"all": nums[0], "cell": nums[1], "bbox": nums[2],
                 "answers": int(re.search(r"distinct start coordinates: (\d+)", txt).group(1))}
data["median_s"] = med
dst = HERE / "anim" / "bbox-prefilter" / "data.json"
dst.write_text(json.dumps(data))
(dst.parent / "data.js").write_text("window.DATA = " + json.dumps(data) + ";\n")
print(f"zones in window: {len(sel)} | stations in window: {len(data['points'])} | pairs: {data['pairs']} | medians: {med}")
print("wrote", dst.relative_to(HERE.parent))
