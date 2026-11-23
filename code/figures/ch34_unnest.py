"""Figure for Chapter 34, Step 10 (UNNEST). Values from code/ch34/output/unnest.txt."""
from pathlib import Path
from svgkit import *

OUT = str(Path(__file__).resolve().parents[2] / "book" / "media" / "figures") + "/"
s = Svg(820, 330, "Two arrays: UNNEST in parallel against two explodes")
s.text(20, 26, "input: tags = [fragile, Gift, gift], n = [10, 20]", 14, MUTE, "start", 600)
# left: UNNEST parallel
s.frame(14, 44, 380, 270, "UNNEST(tags, n) WITH ORDINALITY: 3 rows", BLUE, 13)
for j, (t, n, p) in enumerate([("fragile", "10", "1"), ("Gift", "20", "2"), ("gift", "NULL", "3")]):
    y = 80 + j * 52
    s.box(34, y, 130, 40, t, BLUE, 14)
    s.box(174, y, 100, 40, n, GRAY if n == "NULL" else BLUE, 14)
    s.box(284, y, 90, 40, p, ORANGE, 14)
s.text(204, 290, "row i takes element i of each array", 13, MUTE)
# right: two explodes
s.frame(420, 44, 386, 270, "explode(tags), then explode(n): 6 rows", RED, 13)
pairs = [(t, n) for t in ("fragile", "Gift", "gift") for n in ("10", "20")]
for j, (t, n) in enumerate(pairs):
    y = 72 + j * 36
    s.box(440, y, 150, 30, t, GRAY, 13)
    s.box(600, y, 90, 30, n, GRAY, 13)
s.text(612, 300, "each tag with each number", 13, MUTE)
s.save(OUT + "ch34-unnest-parallel.svg")
print("ok")
