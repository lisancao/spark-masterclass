"""Figures for Chapter 73.
1. The problems of the sample, from the Step 1 table and demo.txt.
2. The micro-batches of the stream over each change log. The byte ranges come from the
   connector's own ChangesStreamReader run on the sample with no Spark
   (code/ch73/anim/stream_plan.json); they agree with demo.txt: batches 7, 7, 8, 7, 7, 7,
   43 rows, and the end offset of batch 0.
"""
import json
from svgkit import *

# 1. problems map
cases = [("CASE-0001", [], "none"), ("CASE-0002", [], "call.wav: 40,044 bytes"),
         ("CASE-0003", [("other", "no final newline")], ""),
         ("CASE-0004", [("cases", "missing_attachment"), ("attachments", "scan.png missing"), ("changes", "unparsed line")], ""),
         ("CASE-0005", [("cases", "unlisted_attachment"), ("attachments", "stray.tmp unlisted")], ""),
         ("CASE-0006", [("attachments", "receipt.pdf sha_mismatch"), ("other", "out of sequence: review")], ""),
         ("CASE-0007", [("ok", "version 1 index")], ""),
         ("CASE-0008", [("cases", "bad_index"), ("attachments", "note.txt unlisted")], ""),
         ("case-0009", [("ok", "BOM, CRLF, +02:00")], ""),
         ("CASE-0010", [("cases", "bad_priority"), ("changes", "unparsed line")], "")]
COL = {"cases": BLUE, "attachments": ORANGE, "changes": GREEN, "ok": TEAL, "other": GRAY}
s = Svg(780, 330, "The problems of the casefile sample and where each one appears")
for i, (cid, probs, note) in enumerate(cases):
    col, row = i % 5, i // 5
    x, y = 12 + col * 153, 20 + row * 140
    s.frame(x, y, 145, 128, "", MUTE, r=8, dash=False)
    s.text(x + 72, y + 20, cid, 13, MUTE, "middle", 700, mono=True)
    if not probs:
        s.text(x + 72, y + 60, note, 11, MUTE, "middle", italic=True)
    for k, (where, what) in enumerate(probs):
        s.box(x + 6, y + 30 + k * 31, 133, 26, what, COL[where], 10)
ly = 312
for i, (lab, c) in enumerate([("cases.problems", BLUE), ("attachments.status", ORANGE), ("changes.problem", GREEN), ("parsed, no problem", TEAL), ("stream or review only", GRAY)]):
    s.rect(12 + i * 152, ly - 10, 14, 12, c, 1, 2)
    s.text(32 + i * 152, ly, lab, 11, MUTE, "start")
s.save(FIG + "ch73-problems.svg")

# 2. micro-batches over the logs
d = json.load(open("../ch73/anim/stream_plan.json"))
order = list(d["sizes"])   # the order of list_case_folders, which the stream follows
BC = [BLUE, ORANGE, GREEN, PURPLE, TEAL, GRAY]
s = Svg(780, 400, "Which micro-batch read each byte range of each change log")
X0, SC = 120, 1.6
for i, f in enumerate(order):
    y = 30 + i * 30
    s.text(X0 - 8, y + 15, f, 11, MUTE, "end", 600, mono=True)
    s.frame(X0, y + 2, d["sizes"][f] * SC, 18, "", MUTE, r=2, dash=False)
for b, batch in enumerate(d["batches"]):
    for folder, a, e, n in batch["ranges"]:
        y = 30 + order.index(folder) * 30
        s.rect(X0 + a * SC, y + 2, (e - a) * SC - 1, 18, BC[b], 1, 2)
        if (e - a) * SC > 24:
            s.text(X0 + (a + e) / 2 * SC, y + 15, str(b), 11, "#ffffff", "middle", 700)
held = d["held"]["CASE-0003"]; size = d["sizes"]["CASE-0003"]
y = 30 + order.index("CASE-0003") * 30
s.rect(X0 + (size - held) * SC, y + 2, held * SC, 18, RED, 0.25, 2, stroke=RED)
s.text(X0 + size * SC + 6, y + 15, f"{held} bytes held: no newline yet", 11, ACC, "start", 600)
ly = 340
for b, batch in enumerate(d["batches"]):
    s.rect(X0 + b * 100, ly - 10, 14, 12, BC[b], 1, 2)
    s.text(X0 + 20 + b * 100, ly, f"batch {b}: {batch['rows']} rows", 11, MUTE, "start")
s.text(X0, 370, "maxBytesPerTrigger 400, availableNow. 43 rows in 6 batches. The batch reader gives 44 rows.", 11, MUTE, "start", italic=True)
s.text(X0, 388, "Byte ranges from the connector's own stream reader, run on the sample with no Spark.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch73-stream-batches.svg")
