"""Helpers for Chapter 76: error text, a tiny shapefile reader, WKB writers and the
NAD83 / New York Long Island (EPSG:2263) projection, all without extra Python packages.
"""
import math
import re
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import DATA  # noqa: E402

ZONES_DIR = DATA / "nyc-taxi" / "zones" / "taxi_zones"
CITIBIKE = DATA / "citibike"
OUT = Path(__file__).resolve().parent / "output"


def err(e, width=70):
    """A one-line description of a Spark error: the condition and the start of the message."""
    from pyspark.errors import PySparkException
    if isinstance(e, PySparkException) and e.getCondition():
        first = next(l for l in str(e).splitlines() if l.strip())
        msg = first.split("] ", 1)[-1]
        if msg.startswith("Cannot resolve"):          # the condition says enough
            return e.getCondition()
        return f"{e.getCondition()}: " + msg[:width]
    cause = re.findall(r"Caused by: ([\w.$]+(?:: [^\n]{0,60})?)", str(e))
    if cause:
        return f"{type(e).__name__}, caused by {cause[-1]}"
    java = re.search(r"(?m)^(?:: )?(?:[\w$]+\.)*([\w$]+(?:Exception|Error)): ([^\n]*(?::\n[^\n]*)?)", str(e))
    if java:
        return f"{java.group(1)}: {' '.join(java.group(2).split())[:width]}"
    return f"{type(e).__name__}: {str(e).splitlines()[0][:width]}"


def attempt(label, fn, width=36):
    from py4j.protocol import Py4JJavaError
    from pyspark.errors import PySparkException
    try:
        print(f"  {label:<{width}} {fn()}")
    except (PySparkException, Py4JJavaError, Exception) as e:  # noqa: BLE001 (we print every failure)
        print(f"  {label:<{width}} {err(e)}")


# ---- shapefile (.shp + .dbf), polygons only -------------------------------------------------

def read_dbf(path):
    """Rows of a dBASE III file as dicts of stripped strings."""
    b = Path(path).read_bytes()
    n, hlen, rlen = struct.unpack("<IHH", b[4:12])
    fields, off = [], 32
    while b[off] != 0x0D:
        name = b[off:off + 11].split(b"\0")[0].decode()
        fields.append((name, b[off + 16]))
        off += 32
    rows = []
    for i in range(n):
        rec = b[hlen + i * rlen: hlen + (i + 1) * rlen]
        pos, row = 1, {}
        for name, size in fields:
            row[name] = rec[pos:pos + size].decode("latin-1").strip()
            pos += size
        rows.append(row)
    return rows


def read_shp(path):
    """Polygon records of a shapefile: a list of rings per record, each ring a list of (x, y)."""
    b = Path(path).read_bytes()
    off, records = 100, []
    while off < len(b):
        _, clen = struct.unpack(">ii", b[off:off + 8])
        rec = b[off + 8: off + 8 + clen * 2]
        off += 8 + clen * 2
        stype = struct.unpack("<i", rec[:4])[0]
        assert stype == 5, f"not a polygon record: {stype}"
        nparts, npts = struct.unpack("<ii", rec[36:44])
        parts = list(struct.unpack(f"<{nparts}i", rec[44:44 + 4 * nparts])) + [npts]
        pts = struct.unpack(f"<{2 * npts}d", rec[44 + 4 * nparts: 44 + 4 * nparts + 16 * npts])
        xy = list(zip(pts[0::2], pts[1::2]))
        records.append([xy[parts[i]:parts[i + 1]] for i in range(nparts)])
    return records


def signed_area(ring):
    return sum(x1 * y2 - x2 * y1 for (x1, y1), (x2, y2) in zip(ring, ring[1:])) / 2


def polygons(rings):
    """Shapefile rule: an outer ring is clockwise (negative area), a hole is counter-clockwise."""
    polys = []
    for r in rings:
        if signed_area(r) < 0 or not polys:
            polys.append([r])
        else:
            polys[-1].append(r)
    return polys


def wkb_multipolygon(polys):
    out = [struct.pack("<BII", 1, 6, len(polys))]
    for p in polys:
        out.append(struct.pack("<BII", 1, 3, len(p)))
        for ring in p:
            out.append(struct.pack("<I", len(ring)))
            out.append(struct.pack(f"<{2 * len(ring)}d", *[c for xy in ring for c in xy]))
    return b"".join(out)


# ---- EPSG:2263, Lambert conformal conic with two standard parallels (EPSG method 9802) ----
# Parameters from taxi_zones.prj: GRS 1980, lat0 40 10', lon0 -74, parallels 41 02' and 40 40',
# false easting 984250 ft, units US survey foot.

A = 6378137.0
F_INV = 298.257222101
E = math.sqrt(2 / F_INV - 1 / F_INV ** 2)
FT = 0.304800609601219
LAT0, LON0, LAT1, LAT2, FE = 40.1666666666667, -74.0, 41.0333333333333, 40.6666666666667, 984250.0


def _m(phi):
    return math.cos(phi) / math.sqrt(1 - (E * math.sin(phi)) ** 2)


def _t(phi):
    s = math.sin(phi)
    return math.tan(math.pi / 4 - phi / 2) / ((1 - E * s) / (1 + E * s)) ** (E / 2)


P1, P2, P0 = (math.radians(v) for v in (LAT1, LAT2, LAT0))
N = (math.log(_m(P1)) - math.log(_m(P2))) / (math.log(_t(P1)) - math.log(_t(P2)))
FF = _m(P1) / (N * _t(P1) ** N)
R0 = A * FF * _t(P0) ** N


def lcc_python(lon, lat):
    """Reference implementation in Python, (lon, lat) degrees to (x, y) US feet."""
    phi = math.radians(lat)
    r = A * FF * _t(phi) ** N
    th = N * math.radians(lon - LON0)
    return (r * math.sin(th)) / FT + FE, (R0 - r * math.cos(th)) / FT


def lcc_columns(lon, lat):
    """The same projection as Spark Column expressions."""
    from pyspark.sql import functions as F
    phi = F.radians(lat)
    s = F.sin(phi)
    t = F.tan(F.lit(math.pi / 4) - phi / 2) / F.pow((1 - E * s) / (1 + E * s), E / 2)
    r = A * FF * F.pow(t, N)
    th = N * F.radians(lon - LON0)
    return (r * F.sin(th)) / FT + FE, (F.lit(R0) - r * F.cos(th)) / FT
