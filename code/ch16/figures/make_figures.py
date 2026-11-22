"""Figures for Chapter 16. Bytes and counts from code/ch16/output/avro.txt and protobuf.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. One record in Avro and in Protobuf (avro.txt, protobuf.txt)
f = Fig(880, 280, "One record in Avro and in Protobuf")
def row(y, title, groups, note):
    f.text(20, y, title, 13, 700, anchor="start")
    x = 20
    for bs, lab, k in groups:
        w = len(bs) * 26
        f.rect(x, y + 10, w - 4, 30, k, r=4)
        for j, b in enumerate(bs):
            f.text(x + j * 26 + 11, y + 30, b, 11.5, 600, c=k, mono=True)
        f.text(x + (w - 4) / 2, y + 56, lab, 10.5, 500, c=k)
        x += w + 4
    f.text(x + 10, y + 30, note, 12, 700, anchor="start", c="mute")
row(28, "Avro: values only, in schema order", [
    (["06", "4f", "2d", "31"], "string 'O-1'", "blue"), (["0e"], "long 7", "ok"),
    (["00", "00", "00", "00", "00", "00", "29", "40"], "double 12.5", "acc"), (["00"], "null", "n")], "14 bytes")
row(120, "Protobuf: a key byte (field number and wire type) before each value", [
    (["0a", "05"], "key 1, len 5", "purple"), (["50", "41", "59", "2d", "31"], "'PAY-1'", "blue"),
    (["12", "03"], "key 2, len 3", "purple"), (["4f", "2d", "31"], "'O-1'", "blue"),
    (["18"], "key 3", "purple"), (["e2", "09"], "1250", "ok"),
    (["20"], "key 4", "purple"), (["01"], "CARD", "ok")], "")
row(186, "", [(["2a", "06"], "key 5, len 6", "purple"), (["6d", "61", "6e", "75", "61", "6c"], "'manual'", "blue")],
    "25 bytes in total. note is NULL, so it has no bytes.")
f.text(20, 272, "The same record as JSON: 43 bytes. Without the schema, nobody can decode the Avro bytes.", 11, 400,
       anchor="start", c="mute", italic=True)
f.save("ch16-record-bytes")

# 2. The Confluent header and the five kinds of message in the orders topic (avro.txt)
f = Fig(860, 260, "The Confluent header and the messages of the orders topic")
f.text(20, 24, "offset 0 of the orders topic", 13, 700, anchor="start")
hdr = [(["00"], "magic", "purple"), (["00", "00", "00", "01"], "schema id 1 (big-endian)", "blue"),
       (["0c", "4f", "2d", "32", "30", "30", "31", "..."], "the Avro record of schema 1", "acc")]
x = 20
for bs, lab, k in hdr:
    w = len(bs) * 28
    f.rect(x, 34, w - 4, 30, k, r=4)
    for j, b in enumerate(bs):
        f.text(x + j * 28 + 12, 54, b, 12, 600, c=k, mono=True)
    f.text(x + (w - 4) / 2 if len(bs) > 1 else x, 80 if len(bs) > 1 else 96, lab, 11, 500, c=k, anchor="middle" if len(bs) > 1 else "start")
    x += w + 4
f.text(20, 122, "30 messages, classified by the header", 13, 700, anchor="start")
kinds = [(12, "schema 1", "ok"), (15, "schema 2", "ok"), (1, "tombstone (NULL value)", "n"),
         (1, "schema id 7: no schema", "bad"), (1, "no header", "bad")]
x = 20
for n, lab, k in kinds:
    w = n * 26
    f.rect(x, 134, w - 2, 34, k, r=4)
    f.text(x + (w - 2) / 2, 156, str(n), 13, 700, c=k)
    x += w
y = 196
x = 20
for n, lab, k in kinds:
    f.rect(x, y - 10, 12, 12, k, r=2)
    f.text(x + 18, y, lab, 11.5, 500, anchor="start", c="mute")
    x += 26 + len(lab) * 6.6
f.text(20, 240, "Red messages go to a dead letter table. from_avro on a value with its header gives wrong values, not an error.",
       11, 400, anchor="start", c="mute", italic=True)
f.save("ch16-confluent")
