"""Chapter 94, Step 8: stop a Python worker at a breakpoint and talk to it over TCP.

breakpoint() in the UDF calls remote_pdb.set_trace (PYTHONBREAKPOINT). The worker waits on
127.0.0.1:REMOTE_PDB_PORT. A thread in this script connects, as a person with `nc` would, and
sends pdb commands. Run from code/:
  uv run --with remote-pdb python ch94/remote_debug.py
"""
import os
import re
import socket
import sys
import threading
import time
from pathlib import Path

PORT = 4444
os.environ["PYTHONBREAKPOINT"] = "remote_pdb.set_trace"    # (the workers inherit the environment
os.environ["REMOTE_PDB_HOST"] = "127.0.0.1"                #  of the local JVM, which inherits ours)
os.environ["REMOTE_PDB_PORT"] = str(PORT)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402
from common.book import session  # noqa: E402

spark = session("ch94-remote-debug", spark_master="local[1]", spark_ui_showConsoleProgress="false")
spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")


@F.udf("double")
def tip_share(tip, fare):
    if fare == 0:
        breakpoint()                   # stop here, only for the bad row
    return round(tip / fare, 3) if fare else None


def debugger():
    for _ in range(100):               # wait until the worker listens
        try:
            s = socket.create_connection(("127.0.0.1", PORT), timeout=30)
            break
        except OSError:
            time.sleep(0.2)
    f = s.makefile("rw")

    def read_prompt():
        buf = ""
        while not buf.endswith("(Pdb) "):
            ch = f.read(1)
            if not ch:
                break
            buf += ch
        return buf

    print("   [debugger] connected to the worker on port", PORT)
    print("   " + re.sub(r"\S*/code/", "", read_prompt().strip()).replace("\n", "\n   ")[:600])
    for cmd in ("p tip, fare", "p __import__('os').getpid()", "w 3", "c"):
        print(f"   (Pdb) {cmd}")
        f.write(cmd + "\n"); f.flush()
        if cmd == "c":
            break
        out = read_prompt().replace("(Pdb) ", "").strip()
        for line in out.splitlines()[-8:]:
            print("     " + re.sub(r"\S*/(site-packages/pyspark/python/lib/pyspark.zip/|code/)", "", line)[:92])
    s.close()


t = threading.Thread(target=debugger)
t.start()
print(f"driver pid {os.getpid()}: run the query; the worker stops at breakpoint()")
rows = spark.createDataFrame([(3.0, 12.5), (1.0, 0.0)], "tip double, fare double").select(
    tip_share("tip", "fare").alias("share")).collect()
t.join()
print(f"result after 'c': {[r.share for r in rows]}")
spark.stop()
