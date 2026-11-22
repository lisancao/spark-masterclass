"""Figures for Chapter 57. Bytes and rows from code/ch57/output/plugins.txt."""
from svgkit import *

# 1. The Any, byte by byte
s = Svg(780, 250, "A jplug.PlanNodes message inside a google.protobuf.Any, byte by byte")
segs = [("0a", "key: field 1\nwire type 2", ORANGE, 46), ("23", "length\n35", GRAY, 40),
        ("type.googleapis.com/jplug.PlanNodes", "type_url (35 bytes)", BLUE, 300), ("12", "key: field 2\nwire type 2", ORANGE, 46),
        ("0e", "length\n14", GRAY, 40)]
x = 20
for txt, lab, c, w in segs:
    s.box(x, 40, w, 34, txt, c, 12, mono=True)
    s.text(x + w / 2, 92, lab, 10, MUTE)
    x += w + 3
inner_x0 = x
inner = [("0a", ORANGE, 30), ("02", GRAY, 30), ("ab", GREEN, 34), ("12", ORANGE, 30), ("08", GRAY, 30), ("physical", GREEN, 84)]
for txt, c, w in inner:
    s.box(x, 40, w, 34, txt, c, 12, mono=True)
    x += w + 3
s.frame(inner_x0 - 4, 32, x - inner_x0 + 5, 50, "", ACC, r=6)
s.text((inner_x0 + x) / 2, 24, "value: the PlanNodes message (14 bytes)", 11, ACC, "middle", 700)
s.text(inner_x0 + 47, 92, "input = b'ab'", 10, MUTE)
s.text(inner_x0 + 174, 92, "stage = 'physical'", 10, MUTE)
s.text(20, 140, "Each field is a key, then a value. key = field_number << 3 | wire_type.", 12, MUTE, "start", 600)
s.text(20, 160, "Wire type 2 (strings, bytes, embedded messages) is a length, then that number of bytes.", 12, MUTE, "start")
s.text(20, 180, "Thus a bytes field and an embedded Relation look the same on the wire.", 12, MUTE, "start")
s.text(20, 215, "Bytes from plugins.py: b'\\n#type.googleapis.com/jplug.PlanNodes\\x12\\x0e\\n\\x02ab\\x12\\x08physical'", 11, MUTE, "start", mono=True)
s.save(FIG + "ch57-any-bytes.svg")

# 2. plan_nodes rows and the tree they describe
s = Svg(780, 260, "The rows of plan_nodes(df) and the optimized plan that they describe")
rows = [(0, "NULL", 0, "Aggregate"), (1, "0", 1, "Project"), (2, "1", 2, "Filter"), (3, "2", 3, "Range")]
for cx, h in zip((20, 60, 140, 210), ("id", "parent", "depth", "node")):
    s.text(cx, 24, h, 13, MUTE, "start", 700, mono=True)
for i, r in enumerate(rows):
    for cx, v in zip((20, 60, 140, 210), r):
        s.text(cx, 52 + i * 30, str(v), 13, MUTE, "start", 400, mono=True)
s.line(300, 100, 380, 100, MUTE, 2)
s.text(340, 90, "as a tree", 11, MUTE)
det = ["Aggregate [k, count(1)]", "Project [(id % 2) AS k]", "Filter (id > 3)", "Range (0, 10)"]
for i, (a, p, d, n) in enumerate(rows):
    x = 420 + d * 26
    s.box(x, 20 + i * 52, 250, 36, det[i], [GRAY, GRAY, BLUE, ORANGE][i], 12, mono=True)
    if i:
        px = 420 + (d - 1) * 26 + 12
        s.path(f"M{px},{20 + (i - 1) * 52 + 36} L{px},{20 + i * 52 + 18} L{x - 2},{20 + i * 52 + 18}", MUTE, 1.5)
s.text(20, 200, "The relation plugin walks the plan depth first on the server", 12, MUTE, "start", 600)
s.text(20, 218, "and returns one row for each node, as a usual DataFrame.", 12, MUTE, "start")
s.text(20, 244, "From plugins.txt: spark.range(10).filter('id > 3').groupBy(id % 2).count(), optimized plan (details shortened).", 11, MUTE, "start", italic=True)
s.save(FIG + "ch57-plan-rows.svg")
