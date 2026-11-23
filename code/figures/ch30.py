"""Figures for Chapter 30. Numbers from code/ch30/output/*.txt."""
from svgkit import *

# 1. What a session isolates and what sessions share (isolation.txt)
s = Svg(780, 400, "One Connect server: per-session state and shared state")
s.frame(10, 10, 760, 380, "Connect server = one Spark application (one driver JVM)", MUTE, 14)
xs = [30, 275, 520]
for i, n in enumerate(["session A", "session B", "session C"]):
    x = xs[i]
    s.box(x, 46, 230, 34, f"{n}   (userId, sessionId)", BLUE, 13)
    s.box(x, 82, 230, 112, "", GRAY, opacity=0.9)
    for j, it in enumerate(["temporary views", "SQL configs", "UDFs", "artifacts", "job tags, operations"]):
        s.text(x + 14, 104 + j * 19, it, 13, "#ffffff", "start")
s.text(395, 216, "isolated: SessionState and artifact manager of each session", 13, ACC, weight=600)
s.box(30, 232, 720, 140, "", ORANGE, opacity=0.9)
s.text(390, 254, "shared by all sessions: SparkContext and SharedState", 14, "#ffffff", weight=700)
shared = [("executors and task slots", "one TaskScheduler (FIFO by default)"),
          ("catalog tables", "B read shared_tbl: 4 rows"),
          ("global temporary views", "B read global_temp.g: 3 rows"),
          ("CacheManager", "B's identical query used A's cache"),
          ("files the server can read", "B read a file under /etc")]
for j, (a, b) in enumerate(shared):
    s.text(50, 280 + j * 19, a, 13, "#ffffff", "start", 600)
    s.text(300, 280 + j * 19, b, 13, "#ffffff", "start", mono=False)
s.save(FIG + "ch30-session-scope.svg")

# 2. FIFO against FAIR (contention.txt), bars per run
s = Svg(780, 330, "Query times under FIFO and FAIR scheduling")
runs = {"FIFO": [(9.59, 11.83), (8.86, 10.96), (8.11, 10.15)],
        "FAIR": [(3.29, 12.01), (1.67, 10.18), (1.72, 10.37)]}
X0, SC = 150, 46  # px per second
s.text(390, 22, "server local[2]: session A runs 16 tasks of 1 s, session B starts a small group-by 2 s later", 13, MUTE)
for t in range(0, 14, 2):
    xx = X0 + t * SC
    s.line(xx, 40, xx, 300, MUTE, 0.6, dash=True, arrow=False)
    s.text(xx, 314, f"{t} s", 11, MUTE)
y = 44
for mode, rr in runs.items():
    s.text(20, y + 52, mode, 18, ACC if mode == "FAIR" else MUTE, "start", 700)
    for k, (b, a) in enumerate(rr):
        s.text(X0 - 10, y + 13, f"run {k+1}", 12, MUTE, "end")
        s.rect(X0, y + 1, a * SC, 14, GRAY, 0.9, 3)
        s.text(X0 + a * SC + 6, y + 13, f"A {a:.2f} s", 12, MUTE, "start")
        s.rect(X0, y + 17, b * SC, 14, BLUE if mode == "FIFO" else GREEN, 1, 3)
        s.text(X0 + b * SC + 6, y + 29, f"B {b:.2f} s", 12, MUTE, "start", 600)
        y += 40
    y += 14
s.save(FIG + "ch30-fifo-fair.svg")

# 3. Time to the first answer (local_reuse.txt, sandbox.txt)
s = Svg(780, 230, "Seconds to the first answer, by how the client gets a server")
rows = [("new local server in each process", 4.4, GRAY, "local_reuse.py, fresh, process 2"),
        ("reused local server (4.3)", 0.4, GREEN, "local_reuse.py, reuse, process 2"),
        ("new sandbox server for an agent", 4.9, ORANGE, "sandbox.py, about 480 MB JVM each"),
        ("new session on a running shared server", 1.64, BLUE, "sandbox.py, no new JVM")]
X0, SC = 300, 70
for i, (lab, v, c, src) in enumerate(rows):
    y = 20 + i * 48
    s.text(X0 - 12, y + 18, lab, 13, MUTE, "end", 600)
    s.rect(X0, y + 4, v * SC, 20, c, 1, 3)
    s.text(X0 + v * SC + 8, y + 19, f"{v} s", 13, MUTE, "start", 700)
    s.text(X0, y + 40, src, 11, MUTE, "start", italic=True)
s.save(FIG + "ch30-first-answer.svg")
