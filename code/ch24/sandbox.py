"""Chapter 24, Step 9: a sandbox for each agent. Two dedicated Connect servers, each with its own
port, warehouse, scratch folder, cores and memory, compared with one session on a shared server.

Run from code/:  uv run python ch24/sandbox.py
"""
import faulthandler
import glob
import shutil
import threading
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ROOT  # noqa: E402
from connect_util import CODE, server, session, close, stop, error_class  # noqa: E402

RUN = CODE.parent / "data"
faulthandler.dump_traceback_later(200, exit=True)   # a stuck client prints its stack, then the script stops


def rss_mb(port):
    pid = open(glob.glob(f"{RUN}/pids/part08-{port}/*.pid")[0]).read().strip()
    for line in open(f"/proc/{pid}/status"):
        if line.startswith("VmRSS"):
            return int(line.split()[1]) // 1024, pid


shutil.rmtree(ROOT / "data" / "ch24" / "sandbox", ignore_errors=True)
sandboxes = {}
for agent, port, ui in (("agent-a", 10284, 4129), ("agent-b", 10285, 4130)):
    home = ROOT / "data" / "ch24" / "sandbox" / agent
    t = time.perf_counter()
    url = server("spark.master=local[1]", "spark.driver.memory=512m",
                 f"spark.sql.warehouse.dir={home}/warehouse", f"spark.local.dir={home}/tmp",
                 "spark.sql.shuffle.partitions=4", port=port, ui=ui)
    s = session(url, user_id=agent)
    s.range(10).count()
    sandboxes[agent] = (s, port, ui)
    mb, pid = rss_mb(port)
    print(f"{agent}: server on :{port}, ready and answered in {time.perf_counter() - t:.1f} s, "
          f"JVM resident memory {mb} MB")

a, b = sandboxes["agent-a"][0], sandboxes["agent-b"][0]
a.sql("CREATE TABLE IF NOT EXISTS scratch AS SELECT id FROM range(5)")
try:
    b.table("scratch").count()
    print("agent-b sees agent-a's table: yes")
except Exception as e:
    print("agent-b sees agent-a's table:", error_class(e))

stop(port=10284, ui=4129)                         # agent-a's sandbox is gone
out = {}


def probe():
    try:
        out["r"] = a.range(1).count()
    except Exception as e:
        out["r"] = error_class(e)


t = threading.Thread(target=probe, daemon=True)
t.start()
t.join(20)
print("agent-a after its server stopped:", out.get("r", "no answer in 20 s (the client keeps retrying)"))
print("agent-b still answers:", b.range(3).count(), "rows")

shared = server()                                 # for comparison: one more session on a shared server
t = time.perf_counter()
c = session(shared, user_id="agent-c")
c.range(10).count()
print(f"a new session on a running shared server answered in {time.perf_counter() - t:.2f} s")
close(b, c)
stop(port=10285, ui=4130)
stop()
sys.stdout.flush()
import os  # noqa: E402
os._exit(0)                                       # agent-a's client keeps a retry thread alive
