"""Figures for Chapter 69. Outcomes from code/ch69/output/packaging.txt."""
from svgkit import *

# 1. Setups and outcomes
s = Svg(780, 300, "Where the package is installed, and what each read gave")
cols = ["driver Python", "worker Python", "format('mailbox')\nno register()", "register(),\nthen count()"]
CX = [40, 170, 330, 560]
for c, x in zip(cols, CX):
    s.text(x, 24, c, 12, MUTE, "start", 700)
rows = [("1. classic", "with", "python3 on PATH", False, "DATA_SOURCE_NOT_FOUND", False, "ModuleNotFoundError"),
        ("2. classic", "with", "with", True, "4 rows", None, ""),
        ("3. classic", "with", "bare", False, "DATA_SOURCE_NOT_FOUND", False, "ModuleNotFoundError"),
        ("4. classic + broken pkg", "with", "with", False, "DATA_SOURCE_NOT_FOUND", True, "4 rows"),
        ("5. Connect", "with (client)", "bare (server)", False, "DATA_SOURCE_NOT_FOUND", False, "ModuleNotFoundError")]
for i, (lab, d, w, ok1, m1, ok2, m2) in enumerate(rows):
    y = 52 + i * 46
    s.text(10, y + 6, lab, 11, MUTE, "start", 600)
    s.box(40, y + 12, 115, 24, d, BLUE, 11, mono=True)
    s.box(170, y + 12, 140, 24, w, BLUE if w in ("with",) else GRAY, 11, mono=True)
    s.box(330, y + 12, 210, 24, m1, GREEN if ok1 else RED, 11, mono=not ok1)
    if ok2 is None:
        s.text(560, y + 29, "(not run)", 11, MUTE, "start")
    else:
        s.box(560, y + 12, 200, 24, m2, GREEN if ok2 else RED, 11, mono=not ok2)
s.text(10, 290, "with: the venv that has pyspark-mailbox. bare: only pyspark. Run 4 also has a pyspark_* package that fails to import.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch69-setups.svg")

# 2. On Connect, register() keeps the includes of that moment
s = Svg(780, 230, "The order of addArtifacts() and register() on Spark Connect")
steps = [("register()", "includes: none", GRAY), ("count()", "ModuleNotFoundError", RED), ("addArtifacts\n(zip)", "session now has the zip", BLUE),
         ("count()", "ModuleNotFoundError", RED), ("register()\nagain", "includes: the zip", BLUE), ("count()", "4 rows", GREEN)]
for i, (a, b, c) in enumerate(steps):
    x = 14 + i * 127
    s.box(x, 46, 115, 48, a, c, 12, mono=True)
    s.text(x + 57, 112, b, 11, MUTE if c != RED else ACC, "middle", 600)
    if i < 5:
        s.line(x + 116, 70, x + 126, 70, MUTE, 2)
s.text(14, 30, "Client session on a Connect server whose workers use 'bare'", 12, MUTE, "start", 700)
s.frame(14, 130, 370, 60, "", RED, r=8)
s.text(199, 155, "the source keeps the includes of the first register()", 11, MUTE)
s.text(199, 172, "(pythonIncludes in UserDefinedPythonDataSource)", 11, MUTE, mono=True)
s.frame(522, 130, 244, 60, "", GREEN, r=8)
s.text(644, 155, "a new register() takes the", 11, MUTE)
s.text(644, 172, "includes of this moment", 11, MUTE)
s.text(14, 220, "Outcomes from run 5 of packaging.sh.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch69-connect-order.svg")
