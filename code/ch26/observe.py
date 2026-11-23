"""Chapter 26: what is this client running right now, and how do I stop it?

Start the server with the event log on:
  ch24/server.sh start --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file://$PWD/../data/ch26-events
Run from code/:  uv run python ch26/observe.py
"""
import base64, json, os, subprocess, sys, threading, time, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F
from common.book import ROOT, TRIPS

PORT = os.environ.get("CONNECT_PORT", "15002")
UI = os.environ.get("CONNECT_UI_PORT", "4041")
URL = f"sc://localhost:{PORT}"
FIG = ROOT / "book" / "media" / "figures"


def rest(path):
    base = f"http://localhost:{UI}/api/v1/applications"
    with urllib.request.urlopen(base) as r:
        app = json.load(r)[0]["id"]
    with urllib.request.urlopen(f"{base}/{app}/{path}") as r:
        return json.load(r)


def short(s):
    return s[:8]


# Two clients: a person in a notebook and an agent. Each gets its own session on the server.
analyst = SparkSession.builder.remote(f"{URL}/;user_id=analyst;user_agent=notebook").create()
agent = SparkSession.builder.remote(f"{URL}/;user_id=agent;user_agent=agent-7").create()
print("== 1. Who is who")
for name, s in [("analyst", analyst), ("agent", agent)]:
    print(f"  {name:8} user_id={s.client._user_id:8} session_id={s.session_id}")

trips = analyst.read.parquet(str(TRIPS))
analyst.addTag("daily-report")
top = trips.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(3)
top.collect()
print(f"  the analyst ran operation {short(top.executionInfo.operation_id)} with tag daily-report")

# The agent starts a long query in a thread, with a tag, and watches its own progress.
progress = []
agent.registerProgressHandler(
    lambda stages, inflight_tasks, operation_id, done: progress.append(
        (round(time.time(), 1), operation_id, sum(s.num_completed_tasks for s in stages),
         sum(s.num_tasks for s in stages), inflight_tasks, done)))
result = {}


def agent_task():
    agent.addTag("task-42")
    try:
        result["count"] = agent.range(0, 10**11, numPartitions=64).where("id % 7 = 3").count()
    except Exception as e:
        result["error"] = f"{type(e).__name__} {e.getCondition()}"
        result["operation_id"] = getattr(e, "operation_id", None)


t = threading.Thread(target=agent_task)
t.start()
time.sleep(6)

print("\n== 2. The operator's view: GET connect/sessions and connect/operations")
for s in rest("connect/sessions"):
    print(f"  session {short(s['sessionId'])}  user {s['userId']:8} operations {s['totalExecution']}"
          f"  finished {bool(s['finishTimestamp'])}")
running = [o for o in rest("connect/operations") if o["state"] in ("STARTED", "COMPILED", "READY")]
for o in running:
    print(f"  RUNNING operation {short(o['operationId'])}  user {o['userId']}  session {short(o['sessionId'])}"
          f"  state {o['state']}  jobs {o['jobIds']}")
    print(f"    tags  {o['sparkSessionTags']}")
    print(f"    statement  {' '.join(o['statement'].split())[:78]} ...")
victim = running[0]

print("\n== 3. The agent's own view: the last progress reports from ExecutionProgress")
for ts, op, done_tasks, tasks, inflight, done in progress[-1:]:
    print(f"  operation {short(op or '')}  tasks {done_tasks}/{tasks} done, {inflight} running, done={done}")

# The Connect tab, while the query runs.
subprocess.run(["google-chrome", "--headless=new", "--disable-gpu", "--hide-scrollbars",
                "--window-size=1600,860", f"--screenshot={FIG / 'ch26-connect-tab.png'}",
                f"http://localhost:{UI}/connect/"], capture_output=True, timeout=60)

print("\n== 4. Stop it: attach to the agent's session from a second process, and interrupt")
operator = SparkSession.builder.remote(
    f"{URL}/;user_id={victim['userId']};session_id={victim['sessionId']}").create()
operator.release_session_on_close = False          # never release the agent's session
print("  operator session_id == agent session_id:", operator.session_id == agent.session_id)
print("  interruptOperation ->", [short(i) for i in operator.interruptOperation(victim["operationId"])])
t.join()
print("  the agent's thread got:", result.get("error"), "| operation", short(result.get("operation_id") or ""))
time.sleep(1)
o = rest(f"connect/operations/detail?jobTag={urllib.parse.quote(victim['jobTag'])}")
print(f"  REST record now: state {o['state']}  jobs {o['jobIds']}  executionTime {o['executionTime']} ms")
print(f"  the tag as the server stores it: {[t for t in o['sparkSessionTags']]}")

print("\n== 5. The server log lines that name the agent's operation")
log = next((ROOT / "data" / "logs").glob("*SparkConnectServer*.out"))
for line in log.read_text().splitlines():
    if victim["operationId"] in line or "cancel jobs with tag" in line:
        logger, msg = line.split(" ", 4)[3:5]
        if logger.startswith(("SparkConnect", "Execute", "DAGScheduler")):
            print(f"  {logger} {msg.replace(victim['sessionId'], short(victim['sessionId']) + '...')[:110]}")

analyst.stop()
agent.stop()
time.sleep(2)

print("\n== 6. The Connect events in the event log, for the agent's session")
import io, zstandard                               # Spark 4 rolls the event log and compresses it with zstd
folder = next((ROOT / "data" / "ch26-events").glob("eventlog_v2_*"))
raw = b"".join(zstandard.ZstdDecompressor().stream_reader(open(f, "rb"), read_across_frames=True).read()
               for f in sorted(folder.glob("events_*")))
t0 = None
for line in raw.decode().splitlines():
    try:
        e = json.loads(line)
    except json.JSONDecodeError:                   # the last line of a file that is still open
        continue
    if e["Event"] == "SparkListenerJobStart" and victim["operationId"] in \
            e.get("Properties", {}).get("spark.job.tags", ""):
        print(f"  +{(e['Submission Time'] - t0):>6} ms  {'SparkListenerJobStart (job ' + str(e['Job ID']) + ')':48}"
              f" spark.job.tags has the operation tag")
        continue
    name = e["Event"].rsplit(".", 1)[-1]
    if "Connect" in name and (e.get("sessionId") == victim["sessionId"]
                              or victim["sessionId"] in e.get("jobTag", "")):
        ts = e.get("eventTime")
        t0 = t0 or ts
        extra = {k: e[k] for k in ("operationId", "userId", "statementText") if k in e}
        extra = {k: (short(v) if k == "operationId" else v) for k, v in extra.items()
                 if k != "statementText"}
        print(f"  +{(ts - t0):>6} ms  {name:48} {extra}")
