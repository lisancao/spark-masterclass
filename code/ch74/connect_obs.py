"""Chapter 74, Steps 7 and 8 (new in 4.3): Spark Connect sessions and operations in the REST API,
live and then from the History Server after the Connect server stopped.

Starts the Part VIII Connect server with the event log on (in the same folder as observed_app.py),
runs two sessions, reads /connect/sessions, /connect/operations and /connect/operations/detail,
stops the server, starts a History Server from the pip install and reads the same endpoints again.

Run from code/, after observed_app.py:  uv run python ch74/connect_obs.py
"""
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ROOT, TRIPS  # noqa: E402
from pyspark.sql.connect.session import SparkSession  # noqa: E402
from pyspark.sql.connect import functions as F  # noqa: E402  (create() sets no active session)

CODE = Path(__file__).resolve().parents[1]
LOGS = ROOT / "data" / "ch74" / "eventlog"
PORT = os.environ.get("CONNECT_PORT", "10280")
UI = os.environ.get("CONNECT_UI_PORT", "4128")
HISTORY = os.environ.get("HISTORY_PORT", "18028")


def get(base, path):
    with urllib.request.urlopen(f"{base}/api/v1/{path}") as r:
        return json.load(r)


def show_connect(base, app):
    sessions = get(base, f"applications/{app}/connect/sessions")
    for s in sessions:
        print(f"  session {s['sessionId'][:8]}  user {s['userId']}  operations {s['totalExecution']}  "
              f"finished {s['finishTimestamp'] > 0}")
    ops = get(base, f"applications/{app}/connect/operations")
    for o in sorted(ops, key=lambda o: o["startTimestamp"]):
        print(f"  operation {o['operationId'][:8]}  session {o['sessionId'][:8]}  {o['state']:8} "
              f"jobs {o['jobIds']}  sql {o['sqlExecIds']}  {o['executionTime']} ms")
    return ops


subprocess.run([str(CODE / "ch72" / "server.sh"), "start",
                "--conf", "spark.eventLog.enabled=true", "--conf", f"spark.eventLog.dir={LOGS}"],
               check=True, stdout=subprocess.DEVNULL)

url = f"sc://localhost:{PORT}"
a = SparkSession.builder.remote(url + "/;user_id=analyst").create()
b = SparkSession.builder.remote(url + "/;user_id=agent").create()
trips = a.read.parquet(str(TRIPS))
a.addTag("daily-report")
top = trips.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(3)
top.collect()
op_ok = top.executionInfo.operation_id
try:
    b.read.parquet(str(TRIPS)).select("revenue").collect()
except Exception as e:
    op_failed = e.operation_id
b.range(10).count()
print("client operation IDs: ok", op_ok[:8], " failed", op_failed[:8])

live = f"http://localhost:{UI}"
app = get(live, "applications")[0]["id"]
print(f"\nlive server {app}, /connect/sessions and /connect/operations:")
ops = show_connect(live, app)

mine = next(o for o in ops if o["operationId"] == op_failed)
detail = get(live, f"applications/{app}/connect/operations/detail?jobTag="
                   + urllib.parse.quote(mine["jobTag"], safe=""))
print("\n/connect/operations/detail?jobTag=... for the failed operation:")
print("  jobTag:", detail["jobTag"][:72] + "...")
print("  statement:", " ".join(detail["statement"].split())[:80])
print("  detail:", detail["detail"].splitlines()[0][:84])
tagged = [j for j in get(live, f"applications/{app}/jobs")
          if any(t.endswith("daily-report") for t in j["jobTags"])]
print("jobs with the client tag 'daily-report':", sorted(j["jobId"] for j in tagged))
print("  the tag as the server stores it:", next(t for t in tagged[0]["jobTags"] if t.endswith("daily-report")))
print("  other tags on the job:", len(tagged[0]["jobTags"]) - 1)

for s in (b, a):
    s.client.release_session()
    s.client.close()
time.sleep(2)
subprocess.run([str(CODE / "ch72" / "server.sh"), "stop"], check=True)

# --- Step 8: the History Server reads the same records from the event log ----------------------
spark_home = os.path.dirname(__import__("pyspark").__file__)
env = dict(os.environ, SPARK_HISTORY_OPTS=f"-Dspark.history.fs.logDirectory={LOGS} "
                                          f"-Dspark.history.ui.port={HISTORY}")
hs = subprocess.Popen([f"{spark_home}/bin/spark-class", "org.apache.spark.deploy.history.HistoryServer"],
                      env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
hist = f"http://localhost:{HISTORY}"
try:
    for _ in range(60):
        try:
            apps = get(hist, "applications")
            if len(apps) >= 2:
                break
        except OSError:
            pass
        time.sleep(1)
    print(f"\nHistory Server on port {HISTORY}, applications:")
    for x in apps:
        print(f"  {x['id']:24} {x['name']:24} completed {x['attempts'][0]['completed']}")
    print(f"\nHistory Server, the Connect records of {app} after the server stopped:")
    show_connect(hist, app)
finally:
    hs.terminate()
    hs.wait()
