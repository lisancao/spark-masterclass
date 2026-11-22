"""Chapter 46, Step 7 (new in 4.3): a persistent local Connect server that later processes reuse.

Runs three short processes that each call remote("local[2]"), first without reuse (each process
starts its own server), then with spark.local.connect.reuse=true. Prints the time from the start
of each process to its first result, and the discovery file without the token.

Run from code/:  flock $HOME/spark-masterclass/data/.bench.lock uv run python ch46/local_reuse.py
"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
PY = sys.executable
work = tempfile.mkdtemp(prefix="ch46-reuse-")
env = dict(os.environ, SPARK_LOCAL_CONNECT_DISCOVERY=f"{work}/connect-local.json")
env.pop("SPARK_REMOTE", None)

for mode in ("fresh", "reuse"):
    print(f"{mode}: seconds to the first result, pid, session, server")
    for i in range(3):
        out = subprocess.run([PY, str(HERE / "reuse_child.py"), mode], env=env,
                             capture_output=True, text=True).stdout.split()
        print(f"  process {i + 1}: {out[0]:>5} s  pid {out[1]}  session {out[2]}  server {out[3]}")

d = json.load(open(f"{work}/connect-local.json"))
print("discovery file:", {k: ("<redacted>" if k == "token" else v) for k, v in sorted(d.items())})
print("discovery folder mode:", oct(os.stat(work).st_mode & 0o777),
      " file mode:", oct(os.stat(f"{work}/connect-local.json").st_mode & 0o777))
stop = subprocess.run([PY, "-m", "pyspark.sql.connect.local_server", "--stop"], env=env,
                      capture_output=True, text=True)
print("python -m pyspark.sql.connect.local_server --stop:", (stop.stdout + stop.stderr).strip()[:100] or "exit 0")
