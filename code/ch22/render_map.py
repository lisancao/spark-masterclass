"""Chapter 22: draw the map figure (trips per taxi zone, June 2026) and the pairs figure as SVG,
from the saved outputs. Needs zones.py, pip.py and pip_bench.py first.

Run from code/:  uv run python ch22/render_map.py
"""
import csv
import math
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow.parquet as pq
from geo76 import OUT
from common.book import work

W = work("ch22", clean=False)
FIG = Path(__file__).resolve().parents[2] / "book" / "media" / "figures"
FIG.mkdir(parents=True, exist_ok=True)
RAMP = ["#b7d3f6", "#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281"]   # sequential blue, light to dark
INK = "#8a8f98"                                                            # mid-tone text, reads on light and dark
EDGE = "#9aa0a8"

zones = pq.read_table(W / "zones.parquet", columns=["location_id", "rings", "xmin", "xmax", "ymin", "ymax"]).to_pylist()
counts = {int(r["location_id"]): r for r in csv.DictReader(open(OUT / "trips_per_zone.csv"))}
X0, X1 = min(z["xmin"] for z in zones), max(z["xmax"] for z in zones)
Y0, Y1 = min(z["ymin"] for z in zones), max(z["ymax"] for z in zones)
PW = 430                                    # panel width in px
S = PW / (X1 - X0)
PH = (Y1 - Y0) * S


def path(rings, ox, oy, tol=900.0):
    out = []
    for r in rings:
        pts = [r[0]]
        for p in r[1:]:
            if abs(p["x"] - pts[-1]["x"]) + abs(p["y"] - pts[-1]["y"]) > tol:
                pts.append(p)
        if len(pts) < 3:
            continue
        out.append("M" + "L".join(f"{ox + (p["x"] - X0) * S:.0f},{oy + (Y1 - p["y"]) * S:.0f}" for p in pts) + "Z")
    return "".join(out)


BINS = [1, 1_000, 5_000, 20_000, 50_000, 100_000, 10 ** 9]
LABELS = ["1 to 999", "1k to 5k", "5k to 20k", "20k to 50k", "50k to 100k", "100k or more"]


def color(n):
    if n <= 0:
        return None
    return next(RAMP[i] for i in range(6) if BINS[i] <= n < BINS[i + 1])


def panel(key, title, ox, oy):
    s = [f'<text x="{ox}" y="{oy - 14}" font-size="17" font-weight="600" fill="{INK}">{title}</text>']
    total = sum(int(c[key]) for c in counts.values())
    s.append(f'<text x="{ox}" y="{oy + 6}" font-size="13" fill="{INK}">{total:,} in June 2026, '
             f'{sum(int(c[key]) > 0 for c in counts.values())} of {len(counts)} zones</text>')
    for z in zones:
        n = int(counts[z["location_id"]][key])
        fill = color(n)
        style = f'fill="{fill}" stroke="{EDGE}" stroke-width="0.4"' if fill else \
            f'fill="none" stroke="{EDGE}" stroke-width="0.6"'
        s.append(f'<path d="{path(z["rings"], ox, oy + 20)}" {style}/>')
    return "\n".join(s)


H = int(PH + 140)
svg = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {2 * PW + 70} {H}" '
       f'font-family="DM Sans, Helvetica, Arial, sans-serif">',
       panel("bike_starts", "Citi Bike trip starts, by point in polygon", 20, 40),
       panel("taxi_pickups", "Yellow taxi pickups, by PULocationID", PW + 50, 40)]
ly = int(PH + 90)
svg.append(f'<text x="20" y="{ly - 12}" font-size="13" fill="{INK}">trips per zone</text>')
for i, (c, lab) in enumerate(zip(RAMP, LABELS)):
    x = 20 + i * 140
    svg.append(f'<rect x="{x}" y="{ly}" width="18" height="12" rx="2" fill="{c}"/>'
               f'<text x="{x + 24}" y="{ly + 11}" font-size="13" fill="{INK}">{lab}</text>')
svg.append(f'<rect x="20" y="{ly + 22}" width="18" height="12" rx="2" fill="none" stroke="{EDGE}"/>'
           f'<text x="44" y="{ly + 33}" font-size="13" fill="{INK}">no trips</text>')
svg.append("</svg>")
(FIG / "ch22-map.svg").write_text("\n".join(svg))
print("wrote book/media/figures/ch22-map.svg", round((FIG / "ch22-map.svg").stat().st_size / 1024), "KiB")

# ---- the pairs figure: candidates per method and the measured medians
txt, bench = (OUT / "pip.txt").read_text(), (OUT / "pip_bench.txt").read_text()
rows = re.findall(r"^\s+(all pairs|same grid cell|same cell and inside bbox)\s+([\d,]+)\s+([\d,]+)", txt, re.M)
med = dict(re.findall(r"^\s+(distinct points, [^\d]+?)\s+[\d,]+\s+([\d.]+)", bench, re.M))
names = ["all pairs", "same grid cell", "same cell and inside bbox"]
times = [med["distinct points, all pairs"], None, med["distinct points, grid + bbox + edges"]]
svg = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 900 250" font-family="DM Sans, Helvetica, Arial, sans-serif">',
       f'<text x="20" y="26" font-size="16" font-weight="600" fill="{INK}">Edge tests for 2,279 stations and 263 zones '
       f'(log scale)</text>']
top = math.log10(int(rows[0][2].replace(",", "")))
for i, (name, pairs, edges) in enumerate(rows):
    e = int(edges.replace(",", ""))
    w = 400 * math.log10(e) / top
    y = 50 + i * 62
    svg.append(f'<text x="20" y="{y + 20}" font-size="14" fill="{INK}">{name}</text>')
    svg.append(f'<rect x="220" y="{y + 4}" width="{w:.1f}" height="24" rx="4" fill="{RAMP[4 - i]}"/>')
    svg.append(f'<text x="{228 + w:.1f}" y="{y + 21}" font-size="13" fill="{INK}">{edges} edge tests, {pairs} pairs</text>')
svg.append(f'<text x="20" y="238" font-size="13" fill="{INK}">median time on the same 2,279 points: all pairs '
           f'{times[0]} s, grid + bbox + edges {times[2]} s (local[*], 16 cores, median of 3)</text>')
svg.append("</svg>")
(FIG / "ch22-pairs.svg").write_text("\n".join(svg))
print("wrote book/media/figures/ch22-pairs.svg")
