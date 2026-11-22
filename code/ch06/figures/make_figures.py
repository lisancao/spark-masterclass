"""Figures for Chapter 6. Numbers from code/ch06/output/strings.txt and values.txt."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. Distinct city spellings under each collation (strings.txt)
data = [("UTF8_BINARY", 29, 26), ("UTF8_LCASE", 20, 15), ("UNICODE_CI", 18, 13),
        ("UNICODE_CI_AI", 15, 9), ("UNICODE_CI_AI_RTRIM", 11, 9)]
f = Fig(800, 300, "Distinct city values under each collation")
f.text(20, 26, "Distinct values of city (166 rows, 9 real cities)", 15, 700, anchor="start")
X0, W = 200, 520
def bx(n): return X0 + W * n / 30
for i, (c, a, b) in enumerate(data):
    y = 50 + i * 44
    f.text(X0 - 10, y + 22, c, 12.5, 600, anchor="end", mono=True)
    f.rect(X0, y + 4, bx(a) - X0, 16, "n", r=3)
    f.text(bx(a) + 6, y + 17, str(a), 11.5, 600, anchor="start", c="mute")
    k = "ok" if b == 9 else "blue"
    f.rect(X0, y + 22, bx(b) - X0, 16, k, r=3)
    f.text(bx(b) + 6, y + 35, f"{b} with trim", 11.5, 700, anchor="start", c=k)
f.line(bx(9), 44, bx(9), 272, c="ok", dash=True)
f.text(bx(9), 286, "9 cities", 12, 700, c="ok")
f.rect(500, 276, 12, 10, "n", r=2); f.text(518, 285, "collation only", 11, 500, anchor="start", c="mute")
f.rect(620, 276, 12, 10, "blue", r=2); f.text(638, 285, "trim, then collation", 11, 500, anchor="start", c="mute")
f.save("ch06-collations")

# 2. The same letters, different bytes (strings.txt)
f = Fig(800, 230, "Two byte forms of Montréal")
f.text(20, 26, "The same letters, different bytes", 15, 700, anchor="start")
rows = [("MONTRÉAL", ["4D", "4F", "4E", "54", "52", "C3 89", "41", "4C"], ["M", "O", "N", "T", "R", "É", "A", "L"], 5, "one character: É"),
        ("Montréal", ["4D", "6F", "6E", "74", "72", "65", "CC 81", "61", "6C"], ["M", "o", "n", "t", "r", "e", "◌́", "a", "l"], (5, 6), "e + combining accent")]
for r, (word, bs, ch, hi, note) in enumerate(rows):
    y = 50 + r * 86
    f.text(20, y + 30, word, 16, 700, anchor="start")
    x = 130
    his = hi if isinstance(hi, tuple) else (hi,)
    for j, (b, c) in enumerate(zip(bs, ch)):
        w = 64 if " " in b else 40
        k = "acc" if j in his else "n"
        f.box(x, y + 10, w, 30, b, kind=k, size=13, mono=True)
        f.text(x + w / 2, y + 58, c, 13, 500, c="acc" if j in his else "mute")
        x += w + 6
    f.text(x + 10, y + 30, note, 12, 600, anchor="start", c="acc")
f.text(20, 218, "UTF8_LCASE compares bytes: two groups. The UNICODE collations compare the two forms as equal.", 12, 400, anchor="start", c="mute", italic=True)
f.save("ch06-bytes")

# 3. Reject reasons in the cleaned customer export (values.txt)
reasons = [("bad_age", 3), ("bad_date", 2), ("bad_email", 2), ("bad_country", 1), ("bad_ltv", 1),
           ("bad_newsletter", 1), ("bad_phone", 1), ("missing_name", 1)]
f = Fig(800, 250, "Reject reasons in the customer export")
f.text(20, 26, "40 rows kept, 5 with a reject_reason", 15, 700, anchor="start")
# 40 squares, 5 marked
for i in range(40):
    x = 20 + (i % 10) * 22
    y = 46 + (i // 10) * 22
    f.rect(x, y, 16, 16, "acc" if i in (3, 5, 12, 25, 33) else "ok", r=3)
f.text(20, 152, "each square is one row", 11, 400, anchor="start", c="mute", italic=True)
f.text(20, 172, "orange: a row with a reason", 11, 500, anchor="start", c="acc")
f.text(20, 190, "the row stays, its bad columns are NULL", 11, 500, anchor="start", c="mute")
X0 = 400
for i, (r, n) in enumerate(reasons):
    y = 40 + i * 24
    f.text(X0 - 8, y + 14, r, 12, 500, anchor="end", mono=True)
    f.rect(X0, y + 3, 90 * n, 15, "acc", r=2)
    f.text(X0 + 90 * n + 6, y + 15, str(n), 12, 700, anchor="start", c="acc")
f.text(X0 + 150, 236, "one row can have several reasons", 11, 400, c="mute", italic=True)
f.note("square positions are schematic", x=230, y=236)
f.save("ch06-reject-reasons")
