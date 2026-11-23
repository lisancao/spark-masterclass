"""Chapter 16: check the hand-written geometry against reference libraries (not used by the book's code).

pyproj checks the EPSG:2263 projection, shapely checks the areas and the point-in-polygon answers.
Needs zones.py and pip.py first.
Run from code/:  uv run --with shapely==2.1.2 --with pyproj==3.7.2 python ch16/check_reference.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyarrow.parquet as pq
import pyproj
import shapely
from shapely import wkb as swkb
from geo76 import lcc_python
from common.book import work

W = work("ch16", clean=False)
print("shapely", shapely.__version__, "| pyproj", pyproj.__version__, "| PROJ", pyproj.proj_version_str)

zones = pq.read_table(W / "zones.parquet", columns=["location_id", "wkb", "area_sqft"]).to_pylist()
pts = pq.read_table(W / "points.parquet", columns=["pid", "start_lng", "start_lat", "x", "y"]).to_pylist()
hits = pq.read_table(W / "pip.parquet", columns=["pid", "location_id"]).to_pylist()

print("\n== projection: geo76.lcc_python against pyproj EPSG:4326 -> EPSG:2263")
t = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:2263", always_xy=True)
worst = max(max(abs(a - b) for a, b in zip(t.transform(p["start_lng"], p["start_lat"]), (p["x"], p["y"])))
            for p in pts)
print(f"points: {len(pts)} | largest difference: {worst:.6f} ft")
print("Times Square:", t.transform(-73.985130, 40.758896), lcc_python(-73.985130, 40.758896))

print("\n== areas: shoelace in Spark against shapely")
geoms = {z["location_id"]: swkb.loads(z["wkb"]) for z in zones}
rel = max(abs(geoms[z["location_id"]].area - z["area_sqft"]) / geoms[z["location_id"]].area for z in zones)
print(f"zones: {len(zones)} | largest relative difference: {rel:.2e} | invalid in shapely: "
      f"{sum(not g.is_valid for g in geoms.values())}")

print("\n== point in polygon: Spark edge crossings against shapely.contains")
spark_ans = {}
for h in hits:
    spark_ans.setdefault(h["pid"], set()).add(h["location_id"])
tree = shapely.STRtree(list(geoms.values()))
ids = list(geoms.keys())
diff = 0
for p in pts:
    pt = shapely.Point(p["x"], p["y"])
    ref = {ids[i] for i in tree.query(pt, predicate="within")}
    if ref != spark_ans.get(p["pid"], set()):
        diff += 1
print(f"points: {len(pts)} | different answers: {diff}")
