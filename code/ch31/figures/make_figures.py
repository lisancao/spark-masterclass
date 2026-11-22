"""Figures for Chapter 31. Numbers from code/ch31/output/media.txt, blobs.txt and documents.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. Pixels are B, G, R (media.txt). The swatch colours are the true colours of the bytes, the same in both themes.
f = Fig(820, 230, "The image source keeps pixels as B, G, R")
f.text(20, 24, "red_16x8.png, first pixel: 00 00 ff", 14, 700, anchor="start", mono=True)
for j, (b, ch) in enumerate([("00", "B"), ("00", "G"), ("ff", "R")]):
    f.box(20 + j * 60, 40, 52, 40, b, kind="n", size=15, mono=True)
    f.text(46 + j * 60, 98, ch, 13, 700, c="mute")
def swatch(x, color, label, sub):
    f.raw(lambda p, x=x, color=color: f'<rect x="{x}" y="40" width="70" height="56" rx="6" fill="{color}" stroke="{p["line"]}"/>')
    f.text(x + 82, 62, label, 13, 700, anchor="start")
    f.text(x + 82, 82, sub, 12, 500, anchor="start", c="mute")
f.arrow(205, 60, 250, 60)
swatch(262, "#ff0000", "read as B, G, R: red", "the true colour")
f.arrow(205, 150, 250, 150)
f.raw(lambda p: f'<rect x="262" y="130" width="70" height="56" rx="6" fill="#0000ff" stroke="{p["line"]}"/>')
f.text(344, 152, "read as R, G, B (Pillow, most models): blue", 13, 700, anchor="start", c="bad")
f.text(344, 172, "reverse the channels before you give the pixels to the library", 12, 500, anchor="start", c="mute")
f.text(20, 154, "the same 3 bytes", 12, 600, anchor="start", c="mute")
f.text(20, 214, "logo_rgba_12x12.png, first pixel: ff 00 00 ff (B, G, R, A). Alpha comes last.", 12, 500, anchor="start", c="mute")
f.save("ch31-bgr")

# 2. 40 blobs of 1 MB inline in Parquet (blobs.txt)
f = Fig(860, 300, "Forty 1 MB blobs inline in one Parquet file")
f.text(20, 24, "file 41.9 MB: one row group, 40 rows", 14, 700, anchor="start")
f.rect(20, 40, 520, 150, "ghost", r=8, dash=True)
f.text(30, 58, "row group 0", 12, 700, anchor="start", c="mute")
f.box(30, 70, 60, 110, "name", "0.000 MB", kind="blue", size=12, subsize=10)
f.box(96, 70, 380, 110, "content", "41.945 MB, no min/max", kind="acc", size=14, subsize=11)
f.box(482, 70, 50, 110, "shelf", "0.000", kind="blue", size=11, subsize=10)
f.text(580, 58, "11 scan tasks", 13, 700, anchor="start")
for i in range(11):
    y = 68 + i * 11.5
    f.rect(580, y, 30 if i else 190, 8, "ok" if i == 0 else "ghost", r=2, dash=i > 0)
f.text(778, 77, "40 rows", 11, 700, anchor="start", c="ok")
f.text(580, 200, "one task gets all 40 rows, ten tasks get none", 12, 600, anchor="start", c="mute")
f.text(20, 220, "A query on name and shelf reads struct<name:string,shelf:int>: column pruning skips content.", 12, 600,
       anchor="start", c="blue")
f.text(20, 244, "Random bytes do not compress: compressed 41.945 MB, raw 41.943 MB.", 12, 500, anchor="start", c="mute")
f.text(20, 268, "A row group cannot split, so a table of large blobs reads with little parallelism.", 12, 500, anchor="start",
       c="mute")
f.save("ch31-parquet-blobs")
