"""Chapter 75, Steps 1-2: which processes run a UDF, and when a worker is reused.

Worker settings are core settings, read when the session starts, so each configuration runs in a
fresh Python process. Run from code/:  uv run python ch75/processes.py
"""
import json, os, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

CHILD = r'''
import json, os, subprocess, sys
sys.path.insert(0, ".")
from pyspark.sql import functions as F
conf = json.loads(sys.argv[1])
from pyspark.sql import SparkSession
b = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
for k, v in conf.items():
    b = b.config(k, v)
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def who(_):
    import builtins                     # state on an imported module survives in a reused worker;
    seen = builtins.__dict__.setdefault("_ch57_rows", [0])   # the UDF's own globals are unpickled per task
    seen[0] += 1
    with open("/proc/self/status") as f:
        vm = next(l.split()[1] for l in f if l.startswith("VmSize"))
    return f"{os.getpid()}:{os.getppid()}:{seen[0]}:{int(vm) // 1024}"
who_udf = F.udf(who, "string", useArrow=False)

jvm = spark.sparkContext._jvm.ProcessHandle.current().pid()
print(f"driver python {os.getpid()}, JVM {jvm}")
seen = set()
for job in range(3):
    rows = [r[0].split(":") for r in spark.range(0, 8, 1, 4).select(who_udf("id")).collect()]
    pids = sorted({r[0] for r in rows})
    seen |= set(pids)
    tasks = max(int(r[2]) for r in rows)
    print(f"  job {job}: worker pids {pids}  parent {sorted({r[1] for r in rows})}  "
          f"most rows seen by one worker {tasks}")
print(f"  distinct worker pids over 3 jobs: {len(seen)}")
out = subprocess.run(["ps", "-o", "args=", "--ppid", str(jvm)], capture_output=True, text=True).stdout
daemon = [l for l in out.splitlines() if "pyspark.daemon" in l]
print("  daemon command:", " ".join(daemon[0].split()[-3:]) if daemon else "none")
'''

for conf in [{}, {"spark.python.worker.reuse": "false"}, {"spark.python.use.daemon": "false"}]:
    print(f"--- {conf or 'defaults'}")
    r = subprocess.run([sys.executable, "-c", CHILD, json.dumps(conf)], capture_output=True, text=True,
                       cwd=Path(__file__).resolve().parents[1])
    print(r.stdout, end="")
    if r.returncode:
        print(r.stderr[-2000:])
