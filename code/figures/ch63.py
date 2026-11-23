"""Figures for Chapter 63. Numbers from code/ch63/output/*.txt."""
import re
from svgkit import *
OUT = "../ch63/output/"

def events(fn):
    ev = []
    for ln in open(OUT + fn):
        m = re.match(r"t\+\s*(\d+)s\s+(etl-[ab]) (.+?)\s{2,}(\S+)", ln.rstrip())
        if m:
            ev.append((int(m[1]), m[2], m[3].strip(), m[4]))
    return ev

def spans(ev, end):
    """pod -> list of (start, stop, phase)"""
    cur, out = {}, {}
    for t, job, pod, ph in ev:
        k = (job, pod)
        if k in cur:
            st, p = cur.pop(k)
            out.setdefault(k, []).append((st, t, p))
        if ph not in ("deleted", "Completed"):
            cur[k] = (t, ph)
    for k, (st, p) in cur.items():
        out.setdefault(k, []).append((st, end, p))
    return out

COL = {"Pending": MUTE, "Running": BLUE}
def gantt(s, ev, y0, end, X0, SC, label):
    sp = spans(ev, end)
    order = [("etl-a", "placeholder (driver)"), ("etl-a", "placeholder (executor)"), ("etl-a", "driver"), ("etl-a", "exec-1"), ("etl-a", "exec-2"),
             ("etl-b", "placeholder (driver)"), ("etl-b", "placeholder (executor)"), ("etl-b", "driver"), ("etl-b", "exec-1"), ("etl-b", "exec-2")]
    rows = [k for k in order if k in sp]
    s.text(10, y0 - 8, label, 14, ACC, "start", 700)
    for i, k in enumerate(rows):
        y = y0 + i * 20
        s.text(X0 - 8, y + 12, f"{k[0]} {k[1]}", 11, MUTE, "end", 600 if k[1] == "driver" else 400)
        for st, sto, ph in sp[k]:
            w = max((sto - st) * SC, 3)
            fill = PURPLE if "placeholder" in k[1] and ph == "Running" else COL.get(ph, GRAY)
            s.rect(X0 + st * SC, y + 2, w, 14, fill, 1 if ph == "Running" else 0.5, 2)
    return y0 + len(rows) * 20

s = Svg(780, 420, "Pods of two Spark jobs in a 2-core YuniKorn queue, without and with gang scheduling")
X0 = 220
plain = events("yunikorn_plain.txt")
yb = gantt(s, plain, 36, 40, X0, 5.2, "no gang scheduling: deadlock (stopped at the time limit)")
for t in (0, 10, 20, 30, 40):
    s.text(X0 + t * 5.2, yb + 14, f"{t} s", 10, MUTE)
gang = events("yunikorn_gang.txt")
y1 = yb + 52
SC = 5.2
yb2 = gantt(s, gang, y1, 105, X0, SC, "gang scheduling: one job at a time, each with its minimum gang")
for t in range(0, 106, 15):
    s.text(X0 + t * SC, yb2 + 14, f"{t} s", 10, MUTE)
ly = yb2 + 34
for i, (lab, c, o) in enumerate([("Pending", MUTE, 0.5), ("Running", BLUE, 1), ("placeholder Running (pause image)", PURPLE, 1)]):
    s.rect(X0 + i * 150, ly - 10, 14, 12, c, o, 2)
    s.text(X0 + i * 150 + 20, ly, lab, 11, MUTE, "start")
s.h = ly + 12
s.save(FIG + "ch63-gang-timeline.svg")

# 2. executors alive over time (celeborn_tracking_430.txt, celeborn_celeborn_421.txt)
def alive(fn):
    pts = []
    for ln in open(OUT + fn):
        m = re.search(r"t\+\s*(\d+)s .*executors alive: (\d+)", ln)
        if m:
            pts.append((int(m[1]), int(m[2])))
    return pts
s = Svg(780, 262, "Executors alive during the shuffle_idle job")
X0, SC, Y0, YS = 90, 9.5, 190, 55
for t in range(0, 70, 10):
    s.line(X0 + t * SC, 30, X0 + t * SC, Y0, MUTE, 0.6, dash=True, arrow=False)
    s.text(X0 + t * SC, Y0 + 16, f"t+{t} s", 10, MUTE)
for n in range(3):
    s.text(X0 - 10, Y0 - n * YS + 4, str(n), 12, MUTE, "end")
s.text(20, 110, "executors\nalive", 12, MUTE, "start")
s.rect(X0 + 9 * SC, 30, 45 * SC, Y0 - 30, MUTE, 0.12)
s.text(X0 + 31.5 * SC, 46, "45 s with no tasks (idle timeout 15 s)", 12, MUTE)
for fn, c, lab, dy in [("celeborn_tracking_430.txt", ORANGE, "shuffle tracking, Spark 4.3.0", -3), ("celeborn_celeborn_421.txt", GREEN, "Celeborn, Spark 4.2.1", 3)]:
    pts = alive(fn)
    d = " ".join(("M" if i == 0 else "L") + f"{X0 + t * SC},{Y0 - n * YS + dy}" for i, (t, n) in enumerate(pts))
    for t, n in pts:
        s.circle(X0 + t * SC, Y0 - n * YS + dy, 5, c)
    last = pts[-1]
s.text(X0 + 56 * SC, Y0 - 2 * YS - 12, "tracking: 2 at t+54 s", 12, ACC, "middle", 700)
s.text(X0 + 63 * SC, Y0 - 1 * YS - 12, "Celeborn: 0, then 1", 12, GREEN, "end", 700)
s.rect(X0, 228, 14, 4, ORANGE); s.text(X0 + 20, 233, "shuffle tracking, Spark 4.3.0", 11, MUTE, "start")
s.rect(X0 + 250, 228, 14, 4, GREEN); s.text(X0 + 270, 233, "Celeborn 0.7.0, Spark 4.2.1", 11, MUTE, "start")
s.text(X0, 252, "Each point is a report of the job. The job does not report between the points.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch63-executors-alive.svg")
