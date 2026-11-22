"""Figures for Chapter 61. Bytes from code/ch61/output/layout.txt, numbers from bench.txt."""
from svgkit import *

# 1. The value and the metadata of the first event of day 1 (layout.txt)
s = Svg(780, 420, "The 27 bytes of metadata and the 44 bytes of value of one event")
def bytes_row(x, y, items, w=26):
    pos = []
    for b, c in items:
        s.box(x, y, w - 2, 26, b, c, 11, mono=True, r=3)
        pos.append(x)
        x += w
    return pos
s.text(12, 22, "metadata (27 bytes): header 01, size 04, five offsets, then the names with no separators", 12, ACC, "start", 700)
meta = [("01", ORANGE), ("04", GRAY)] + [(h, BLUE) for h in ["00", "05", "0c", "12", "14"]]
mp = bytes_row(12, 32, meta)
names = [("event", 5), ("user_id", 7), ("amount", 6), ("ts", 2)]
x = mp[-1] + 34
npos = []
for k, (n, ln) in enumerate(names):
    w = max(ln * 14, 36)
    s.box(x, 32, w - 2, 26, n, GREEN, 11, mono=True, r=3)
    s.text(x + w / 2, 74, f"id {k}", 10, MUTE)
    npos.append(x + w / 2)
    x += w
val = [("02", ORANGE), ("04", GRAY)] + [(h, PURPLE) for h in ["02", "00", "03", "01"]] + [(h, BLUE) for h in ["07", "00", "0d", "05", "21"]]
vp = bytes_row(12, 150, val)
lx = vp[-1] + 40
s.text(lx, 142, "value (44 bytes)", 12, ACC, "start", 700)
for j, (t, c) in enumerate([("02: OBJECT, 1-byte widths;  04: four fields", MUTE), ("purple: field ids, in the sort of the names", PURPLE), ("blue: offsets into the data (the last is its length, 33)", BLUE)]):
    s.text(lx, 158 + j * 15, t, 10, MUTE, "start", 600)
s.text(530, 272, "data (33 bytes),", 12, ACC, "start", 700)
s.text(530, 288, "in the order of the JSON text", 11, MUTE, "start", 600)
data = [("11 view", "event = 'view'", 76, 0), ("0c 00", "user_id = 0", 60, 5), ("20 02 65 02 00 00", "amount = 6.13", 150, 7), ("4d 2026-10-01T10:00:00", "ts: a short string of 19 bytes", 210, 13)]
x = 12
dpos = {}
for txt, lab, w, off in data:
    s.box(x, 262, w - 3, 26, txt, TEAL, 10, mono=True, r=3)
    s.text(x + w / 2, 304, lab, 10, MUTE)
    s.text(x + 2, 258, f"+{off}", 9, MUTE, "start")
    dpos[off] = x + w / 2
    x += w
ids = [2, 0, 3, 1]
offs = [7, 0, 13, 5]
for k in range(4):
    xi = vp[2 + k] + 12
    s.path(f"M{xi},150 C{xi},115 {npos[ids[k]]},115 {npos[ids[k]]},82", PURPLE, 1.2)
for k in range(4):
    xo = vp[6 + k] + 12
    s.path(f"M{xo},177 C{xo},225 {dpos[offs[k]]},215 {dpos[offs[k]]},260", BLUE, 1.2)
s.text(12, 340, "Field k: its name is the metadata entry of id k, and its data starts at offset k.", 11, MUTE, "start", italic=True)
s.text(12, 356, "The ids and offsets follow the sort of the names (amount, event, ts, user_id). The data keeps the JSON order.", 11, MUTE, "start", italic=True)
s.text(12, 384, "Header bytes: low 2 bits = basic type, high 6 bits = type info. 0x11 = SHORT_STR, length 4.", 11, MUTE, "start")
s.text(12, 400, "0x0c = PRIMITIVE INT1. 0x20 = PRIMITIVE DECIMAL4 (scale 2, unscaled 613). 0x4d = SHORT_STR, length 19.", 11, MUTE, "start")
s.save(FIG + "ch61-bytes.svg")

# 2. bench.txt
rows = [("JSON text, get_json_object", 6.35, GRAY), ("JSON text, parse_json + variant_get", 9.43, RED), ("VARIANT, variant_get", 1.98, BLUE),
        ("VARIANT shredded, variant_get", 0.68, GREEN), ("VARIANT, to_json of the whole value", 8.91, ORANGE)]
s = Svg(780, 290, "Median seconds of five queries over one million trips, and the file sizes")
X0, SC = 280, 48
for t in range(0, 11, 2):
    s.line(X0 + t * SC, 16, X0 + t * SC, 216, MUTE, 0.6, dash=True, arrow=False)
    s.text(X0 + t * SC, 230, f"{t} s", 10, MUTE)
for i, (lab, v, c) in enumerate(rows):
    y = 20 + i * 38
    s.text(X0 - 8, y + 16, lab, 12, MUTE, "end", 600)
    s.rect(X0, y + 4, v * SC, 18, c, 1, 3)
    s.text(X0 + v * SC + 6, y + 18, f"{v} s", 12, MUTE, "start", 700)
s.text(12, 256, "Files: JSON text 59.6 MiB, VARIANT 41.6 MiB, VARIANT shredded 20.4 MiB.", 12, MUTE, "start", 600)
s.text(12, 276, "One warm-up run, then the median of five, local[*] on 16 cores. Use the ratios, not the absolute times.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch61-bench.svg")
