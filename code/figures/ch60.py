"""Figures for Chapter 60. PIDs and bytes from code/ch60/output/processes.txt and tap.txt."""
from svgkit import *

# 1. The process tree in three settings (processes.txt)
s = Svg(780, 300, "The processes of a UDF query in three settings, with their PIDs")
panels = [("defaults", "812921", "812941", "daemon 814789", ["814942", "814943"], "2 workers for 12 tasks; rows seen by one worker: 4, 8, 12"),
          ("worker.reuse = false", "815191", "815205", "daemon 815986", ["4 new forks", "per job"], "12 workers for 12 tasks"),
          ("use.daemon = false", "816380", "816388", None, ["4 new Pythons", "per job"], "12 workers, each a child of the JVM")]
for i, (title, drv, jvm, dmn, ws, note) in enumerate(panels):
    x = 12 + i * 256
    s.frame(x, 10, 244, 280, title, MUTE, 13)
    s.box(x + 12, 40, 220, 34, "driver Python", GRAY, 12, sub=f"pid {drv}", subsize=10)
    s.box(x + 12, 88, 220, 34, "JVM (driver + executor)", GRAY, 12, sub=f"pid {jvm}", subsize=10)
    y = 136
    if dmn:
        s.box(x + 42, y, 160, 34, "pyspark.daemon", BLUE, 12, sub=dmn.split()[1], subsize=10)
        s.line(x + 122, 123, x + 122, y - 2, MUTE, 1.5)
        y += 50
        s.line(x + 122, y - 16, x + 70, y - 2, MUTE, 1.5); s.line(x + 122, y - 16, x + 174, y - 2, MUTE, 1.5)
    else:
        s.line(x + 122, 123, x + 70, y - 2, MUTE, 1.5); s.line(x + 122, 123, x + 174, y - 2, MUTE, 1.5)
    for k, w in enumerate(ws):
        s.box(x + 12 + k * 116, y, 104, 34, "worker", ORANGE, 12, sub=w, subsize=10)
    s.text(x + 122, y + 60, note, 10, MUTE, "middle", italic=True) if len(note) < 40 else [s.text(x + 122, y + 56 + j * 14, part, 10, MUTE, "middle", italic=True) for j, part in enumerate(note.split("; "))]
s.save(FIG + "ch60-processes.svg")

# 2. The response of the pickled UDF, field by field (tap.txt)
s = Svg(780, 240, "The 102 bytes that the worker sent back for the pickled UDF")
segs = [("00 00 00 22", "frame length\n34", GRAY, 70), ("pickle: ['AB0', 'AB1', 'AB2']", "34 bytes, protocol 5", GREEN, 200),
        ("ff ff ff fd", "TIMING_DATA\n-3", ORANGE, 70), ("4 longs", "boot, init, finish,\nprocessing time", BLUE, 92),
        ("2 longs", "spill: memory, disk\n(0, 0)", BLUE, 70), ("ff ff ff ff", "END_OF_DATA\n_SECTION -1", ORANGE, 70),
        ("00 00 00 00", "accumulator\nupdates: 0", GRAY, 70), ("ff ff ff fc", "END_OF_STREAM\n-4: reuse me", ORANGE, 70)]
x = 12
for k, (txt, lab, c, w) in enumerate(segs):
    s.box(x, 40, w, 40, txt, c, 10, mono=True)
    ly = 98 if k % 2 == 0 else 132
    s.line(x + w / 2, 82, x + w / 2, ly - 12, MUTE, 1, arrow=False)
    s.text(x + w / 2, ly, lab, 10, MUTE)
    x += w + 3
s.text(12, 26, "worker → JVM", 12, ACC, "start", 700)
s.text(12, 176, "All numbers are big-endian. A positive int is the length of the next frame.", 12, MUTE, "start", 600)
s.text(12, 196, "A negative int is a marker from SpecialLengths. -2 (PYTHON_EXCEPTION_THROWN) replaces the results when the UDF raises.", 12, MUTE, "start")
s.text(12, 226, "Bytes from tap.py and wire.py on pyspark 4.3.0.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch60-response.svg")

# 3. RLIMIT_AS counts address space (memory.txt)
s = Svg(780, 226, "Virtual address space and resident memory of a worker, against spark.executor.pyspark.memory")
rows = [("pickled worker, limit 300m", 68, 52, 300), ("Arrow worker, limit 300m", 297, 87, 300), ("Arrow worker, limit 1g, after 400 MiB", 470, 128, 1024)]
X0, SC = 270, 0.42
for i, (lab, vm, rss, lim) in enumerate(rows):
    y = 20 + i * 62
    s.text(X0 - 10, y + 22, lab, 12, MUTE, "end", 600)
    s.rect(X0, y + 6, vm * SC, 22, BLUE, 1, 3)
    s.rect(X0, y + 6, rss * SC, 22, GREEN, 1, 3)
    s.line(X0 + lim * SC, y, X0 + lim * SC, y + 36, RED, 2.5, arrow=False)
    s.text(X0 + lim * SC - 5, y + 2, f"RLIMIT_AS {lim} MiB", 10, RED, "end", 700)
    s.text(X0, y + 44, f"VmSize {vm} MiB, RSS {rss} MiB", 10, MUTE, "start")
ly = 210
for i, (lab, c) in enumerate([("resident (RSS)", GREEN), ("address space (VmSize)", BLUE), ("the limit", RED)]):
    s.rect(X0 + i * 170, ly - 10, 14, 12, c, 1, 2)
    s.text(X0 + 20 + i * 170, ly, lab, 11, MUTE, "start")
s.save(FIG + "ch60-rlimit.svg")
