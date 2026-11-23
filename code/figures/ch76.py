"""Figures for Chapter 76. Numbers and names from code/ch76/output/*.txt."""
from svgkit import *

# 1. Restart timeline of flaky (failing-app.txt)
s = Svg(780, 250, "Three attempts of the flaky application under restartPolicy OnFailure")
X0, SC = 110, 6.6   # px per second
ev = [  # attempt, start, end of the RunningHealthy span (next state), from failing-app.txt
    (0, 1.2, 16.0, 30.4), (1, 36.2, 46.5, 61.2), (2, 66.1, 76.5, 90.5)]
for t in range(0, 100, 10):
    s.line(X0 + t * SC, 30, X0 + t * SC, 200, MUTE, 0.6, dash=True, arrow=False)
    s.text(X0 + t * SC, 216, f"{t} s", 11, MUTE)
for a, st, rh, end in ev:
    y = 40 + a * 52
    s.text(X0 - 12, y + 22, f"attempt {a}", 13, MUTE, "end", 600)
    s.text(X0 - 12, y + 37, f"flaky-{a}-driver", 10, MUTE, "end", mono=True)
    s.box(X0 + st * SC, y + 6, (rh - st) * SC, 26, "driver start", GRAY, 11)
    s.box(X0 + rh * SC, y + 6, (end - rh) * SC, 26, "RunningHealthy", BLUE, 11)
    if a < 2:
        s.text(X0 + end * SC + 5, y + 23, f"ScheduledToRestart {end} s", 11, MUTE, "start", 600)
    else:
        s.text(X0 + end * SC, y + 46, f"ResourceReleased {end} s: no restart left", 11, RED, "end", 600)
    if a < 2:
        nxt = ev[a + 1][1]
        s.box(X0 + end * SC, y + 32 + 20, (nxt - end) * SC, 0.1, "", ORANGE)
        s.path(f"M{X0 + end * SC},{y + 32} C{X0 + end * SC + 10},{y + 70} {X0 + nxt * SC - 10},{y + 40} {X0 + nxt * SC},{y + 58}", ORANGE, 1.5)
        s.text((X0 + (end + nxt) / 2 * SC) + 2, y + 66, "backoff", 10, ACC, "middle", 600)
s.text(390, 240, "restartPolicy OnFailure, maxRestartAttempts 2, restartBackoffMillis 5000 (states polled once a second)", 11, MUTE, italic=True)
s.save(FIG + "ch76-restart-timeline.svg")

# 2. Where the executors run: k8s master against SparkCluster (zones-app.txt, run_cluster.txt, zones-on-cluster.txt)
s = Svg(780, 300, "Pods of the zones application in the two modes")
def pod(x, y, w, name, fill, sub=None, h=40):
    s.box(x, y, w, h, name, fill, 12, mono=True, sub=sub, subsize=11)
s.frame(10, 10, 370, 280, "SparkApplication, master k8s://", MUTE, 13)
pod(30, 50, 330, "zones-0-driver", ORANGE, "driver pod (the operator made it)")
s.line(195, 92, 85, 150); s.line(195, 92, 305, 150)
s.text(195, 122, "the driver asks the\nKubernetes API", 11, MUTE)
pod(30, 152, 160, "zones-exec-1", BLUE, "1,000,000 rows", 46)
pod(200, 152, 160, "zones-exec-2", BLUE, "1,000,000 rows", 46)
s.text(195, 230, "executor pods: created and deleted", 12, MUTE)
s.text(195, 246, "for each application", 12, MUTE)
s.frame(400, 10, 370, 280, "SparkApplication on SparkCluster book", MUTE, 13)
pod(420, 50, 160, "zones-standalone-\n0-driver", ORANGE, None, 46)
pod(590, 50, 160, "book-master-0", GRAY, "spark://...:7077", 46)
s.line(580, 73, 590, 73, MUTE, 2)
for i, x in enumerate([420, 590]):
    s.frame(x, 118, 160, 110, f"book-worker-{i}", MUTE, 12, 8, dash=False, anchor="middle")
    s.box(x + 14, 152, 132, 58, "executor process", BLUE, 11, sub="1,000,000 rows", subsize=11)
s.line(670, 98, 500, 118); s.line(670, 98, 670, 118)
s.text(585, 250, "no executor pods: executors run inside", 12, MUTE)
s.text(585, 266, "the long-lived worker pods (spark.cores.max 4)", 12, MUTE)
s.save(FIG + "ch76-executor-placement.svg")
