"""Chapter 80 figures: where the driver runs in each deploy mode, the master failover timeline, and
how Spark merges an executor pod template. Reads code/ch80/output/*.txt.

Run from code/:  FIG_CHECK_DIR=<scratch> uv run python ch80/figures.py
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.svgfig import Fig  # noqa: E402

OUT = Path(__file__).resolve().parent / "output"


def modes():
    client = (OUT / "submit-client.txt").read_text()
    cl = (OUT / "submit-cluster-jar.txt").read_text()
    k8s = (OUT / "k8s-cluster.txt").read_text()
    inc = (OUT / "k8s-client-in-cluster.txt").read_text()
    f = Fig(940, 470, "Where the driver runs")
    f.text(20, 28, "Where the driver runs, in the four submits of this chapter", 17, 700, anchor="start")
    cols = [
        ("standalone, client", "submit-client.txt", "spark-submit JVM\n= the driver", "laptop", "2 executors on\n2 workers", "acc"),
        ("standalone, cluster", "submit-cluster-jar.txt",
         "spark-submit exits", "worker " + re.search(r"Driver running on (\S+)", cl)[1],
         "executors on\nthe workers", "blue"),
        ("Kubernetes, cluster", "k8s-cluster.txt", "spark-submit waits\n(watches the pod)",
         "pod " + re.search(r"driver host\s+(\S+)", k8s)[1][:18] + "...", "2 executor pods", "purple"),
        ("Kubernetes, client\n(driver in your pod)", "k8s-client-in-cluster.txt", "your pod runs\nspark-submit",
         "pod " + re.search(r"driver host\s+(\S+)", inc)[1], "2 executor pods\nowned by that pod", "ok"),
    ]
    for i, (title, src, sub, drv, ex, kind) in enumerate(cols):
        x = 20 + i * 230
        f.box(x, 50, 210, 54, title, kind="n", size=14)
        f.box(x, 130, 210, 62, "submitting process", sub, kind="ghost", size=13, subsize=12)
        f.box(x, 225, 210, 62, "driver", drv, kind=kind, size=14, subsize=12, sub_mono=True)
        f.box(x, 320, 210, 62, "executors", ex, kind="n", size=13, subsize=12)
        f.arrow(x + 105, 192, x + 105, 225, "")
        f.arrow(x + 105, 287, x + 105, 320, "")
        f.text(x + 112, 308, "tasks", 11, 500, anchor="start", c="mute")
        f.text(x + 105, 405, src, 11, 400, c="mute", mono=True)
    f.text(20, 440, "In client mode the submitting process is the driver. In cluster mode the cluster starts the driver,"
           " and the submitting process only reports.", 12.5, 400, anchor="start", c="mute")
    f.save("ch80-deploy-modes")


def failover():
    t = (OUT / "ha.txt").read_text()
    ev = {k: float(v) for v, k in re.findall(r"^\s*([\d.]+)s\s+(app running|master pid|master started|master ALIVE|app exited)", t, re.M)}
    rec = re.search(r"Recovery complete in (\d+) ms", t)[1]
    f = Fig(940, 345, "Master failover with FILESYSTEM recovery")
    f.text(20, 28, "The master dies and comes back. The application does not notice (ha.txt)", 17, 700, anchor="start")
    x0, x1, tmax = 150, 900, 56.0
    X = lambda s: x0 + (x1 - x0) * s / tmax
    rows = [("master", 70), ("application", 130), ("executors", 190)]
    for name, y in rows:
        f.text(20, y + 22, name, 14, 600, anchor="start")
    kill, start, alive, end = ev["master pid"], ev["master started"], ev["master ALIVE"], ev["app exited"]
    f.rect(X(0), 70, X(kill) - X(0), 34, "ok", r=4)
    f.text((X(0) + X(kill)) / 2, 92, "ALIVE", 12, 600, c="ok")
    f.rect(X(kill), 70, X(start) - X(kill), 34, "bad", r=4, dash=True)
    f.text((X(kill) + X(start)) / 2, 92, "no master", 12, 600, c="bad")
    f.rect(X(start), 70, X(alive) - X(start), 34, "acc", r=4)
    f.rect(X(alive), 70, X(end) - X(alive), 34, "ok", r=4)
    f.text((X(alive) + X(end)) / 2, 92, f"ALIVE (recovery {rec} ms)", 12, 600, c="ok")
    f.rect(X(ev["app running"]) - 60, 130, X(end) - X(ev["app running"]) + 60, 34, "blue", r=4)
    f.text((X(kill) + X(end)) / 2, 152, "RUNNING: the hold loop runs a job each second", 12, 600, c="blue")
    f.rect(X(ev["app running"]) - 60, 190, X(end) - X(ev["app running"]) + 60, 34, "n", r=4)
    f.text((X(kill) + X(end)) / 2, 212, "2 executors the whole time (they talk to the driver)", 12, 600)
    for s, lab in ((kill, "SIGKILL"), (start, "start-master.sh"), (end, "app exits, code 0")):
        f.line(X(s), 60, X(s), 240, c="line", dash=True)
        f.text(X(s), 258, f"{s:.1f} s", 12, 600, c="mute")
        f.text(X(s), 274, lab, 11, 400, c="mute")
    f.text(20, 300, "The driver logs \"Disconnected from Spark cluster! Waiting for reconnection...\","
           " then \"Master has changed\".\nThe new master reads the application and the workers from"
           " spark.deploy.recoveryDirectory.", 12, 400, anchor="start", c="mute")
    f.save("ch80-failover")


def template():
    t = (OUT / "k8s-cluster.txt").read_text()
    grab = lambda k: re.search(rf"^{re.escape(k)}\s+(.*)$", t, re.M)[1].strip()
    f = Fig(940, 450, "Executor pod: template and Spark")
    f.text(20, 28, "One executor pod: what came from the template, and what Spark set (k8s-cluster.txt)", 17, 700, anchor="start")
    f.code(20, 50, 380, open(Path(__file__).parent / "k8s/executor-template.yaml").read().splitlines()[4:24],
           size=11.5, title="executor-template.yaml (from line 5)")
    rows = [["field", "value in the running pod"],
            ["label book.chapter", grab("label book.chapter:")],
            ["env BOOK_TEMPLATE", grab("env BOOK_TEMPLATE:")],
            ["volume spark-local-dir-1", grab("volume spark-local-dir-1:")],
            ["container", grab("container:")],
            ["image", grab("image:")],
            ["args", grab("args:")],
            ["resources", "requests cpu " + re.search(r'"cpu":"([^"]+)"', grab("resources:"))[1]
             + ", memory " + re.search(r'"memory":"([^"]+)"', grab("resources:"))[1]],
            ["SPARK_LOCAL_DIRS", grab("SPARK_LOCAL_DIRS:")],
            ["owner", grab("owner:")[:40]]]
    f.table(420, 50, [170, 330], rows, rh=30, size=12, mono_cols=(1,), ok_rows=(1, 2, 3), hi_rows=(4, 5, 6, 7, 8, 9),
            hi_kind="blue")
    f.box(420, 370, 160, 26, "from the template", kind="ok", size=12)
    f.box(590, 370, 160, 26, "set by Spark", kind="blue", size=12)
    f.text(420, 420, "Memory 1433Mi = 1g heap + 409Mi overhead (factor 0.4 for a Python application).", 12, 400,
           anchor="start", c="mute")
    f.save("ch80-pod-template")


modes()
failover()
template()
