"""Chapter 25: every RPC of a short Connect session, through a logging proxy.

Start the server first:  ch24/server.sh start
Run from code/:          uv run python ch25/protocol.py
"""
import json, os, sys, threading, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession, functions as F
from common.book import ROOT, TRIPS
import proxy

PORT = int(os.environ.get("CONNECT_PORT", "15002"))
PROXY = int(os.environ.get("PROXY_PORT", "25002"))   # the logging proxy listens here
OUT = Path(__file__).parent / "output"
WIRE = ROOT / "data" / "ch25-wire"          # raw messages that the proxy saves (not in git)
WIRE.mkdir(parents=True, exist_ok=True)
for f in WIRE.glob("*"):
    f.unlink()

server, px = proxy.start(PROXY, f"localhost:{PORT}", WIRE / "rpc.jsonl", save_dir=WIRE)
url = f"sc://localhost:{PROXY}/;user_id=reader;user_agent=ch25"   # the client talks to the proxy


def step(name):
    """Write a marker into the RPC log, so that each RPC is listed under the client call that made it."""
    with px.lock:
        px.log.write(json.dumps({"step": name}) + "\n")


spark = SparkSession.builder.remote(url).create()
step("create()")

trips = spark.read.parquet(str(TRIPS))
top = trips.groupBy("PULocationID").agg(F.count("*").alias("trips")).orderBy(F.desc("trips")).limit(3)
step("build a DataFrame")                  # no RPC: the plan only exists in the client

top.columns
step("df.columns")
spark.conf.set("spark.sql.shuffle.partitions", "16")
step("conf.set")
rows = top.collect()
step("df.collect()")
print("top zones:", [tuple(r) for r in rows])

try:
    trips.select("revenue").collect()
except Exception as e:
    print("error:", type(e).__name__, str(e).split(".")[0][:70])
step("collect() with an error")

helper = WIRE / "fares.py"
helper.write_text("def with_tip(fare, tip):\n    return fare + tip\n")
spark.addArtifacts(str(helper), pyfile=True)
step("addArtifacts(pyfile)")


def long_query():
    spark.addTag("nightly")                 # tags belong to the thread that adds them
    try:
        spark.range(0, 10**12, numPartitions=64).where("id % 7 = 3").count()
    except Exception as e:
        print("long query:", type(e).__name__, str(e).split("]")[0] + "]")


t = threading.Thread(target=long_query)
t.start()
time.sleep(3)
print("interrupted:", len(spark.interruptTag("nightly")), "operation(s)")
t.join()
step("interruptTag")

spark.stop()
step("stop()")
time.sleep(1)
server.stop(1)

# The log, grouped by the client call that caused each RPC. A marker comes after the RPCs of its call.
records = [json.loads(line) for line in (WIRE / "rpc.jsonl").read_text().splitlines()]
rows, pending = [], {}
for r in records:
    if "step" in r:
        for n, c in pending.items():
            rows.append((r["step"], n, c))
        pending = {}
        continue
    c = pending.setdefault(r["call"], {"method": r["method"], "req": 0, "resp": 0, "rb": 0, "sb": 0,
                                       "kinds": {}, "error": None})
    if r["dir"] == "req":
        c["req"] += 1; c["rb"] += r["bytes"]
    elif r["dir"] == "resp":
        c["resp"] += 1; c["sb"] += r["bytes"]
        k = r["summary"].replace("response_type=", "").split(" rows=")[0] or "-"
        c["kinds"][k] = c["kinds"].get(k, 0) + 1
    else:
        c["error"] = r["summary"]

print(f"\n{'client call':24} {'RPC':22} {'bytes out/in':>14}  responses")
merged = []
for stepname, n, c in rows:          # fold repeated ReleaseExecute calls into one row
    if merged and merged[-1][0] == stepname and merged[-1][1]["method"] == c["method"] == "ReleaseExecute":
        m = merged[-1][1]; m["times"] += 1; m["rb"] += c["rb"]; m["sb"] += c["sb"]
    else:
        merged.append((stepname, dict(c, times=1)))
last = None
for stepname, c in merged:
    label = stepname if stepname != last else ""
    last = stepname
    name = c["method"] + (f" x{c['times']}" if c["times"] > 1 else "")
    kinds = ", ".join(f"{k} x{v}" if v > 1 else k for k, v in c["kinds"].items())
    if c["error"]:
        kinds = (kinds + ", " if kinds else "") + "error " + c["error"]
    print(f"{label:24} {name:22} {c['rb']:>6}/{c['sb']:<7}  {kinds[:60]}")

print("\nThe response stream of df.collect(), message by message:")
coll = next(n for stepname, n, c in rows if stepname == "df.collect()" and c["method"] == "ExecutePlan")
for r in records:
    if r.get("call") == coll and r["dir"] == "resp":
        print(f"  {r['bytes']:>5} B  {r['summary'].replace('response_type=', '')}")
