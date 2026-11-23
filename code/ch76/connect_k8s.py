"""Chapter 76, Step 5: a Python client on the laptop, a Connect server in the kind cluster.

Port-forwards the driver service of sparkapp/connect, runs a query and a Python UDF, reads the 4.3
Connect REST endpoints, then deletes the driver pod and watches the operator start a new attempt.

Run from code/, after:  kubectl --context kind-part08 apply -f ch76/connect-server.yaml
                        uv run python ch76/connect_k8s.py
"""
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request

from kube import CTX, kubectl, resource, pods
from pyspark.sql.connect.client.retries import DefaultPolicy
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect import functions as F
from pyspark.sql.types import StringType

TOKEN, GRPC, UI = "book-token-123", 10283, 4129


def forward(attempt):
    """kubectl port-forward to the driver service of one attempt, tried again until the driver answers."""
    for _ in range(30):
        p = subprocess.Popen(["kubectl", "--context", CTX, "port-forward", f"svc/connect-{attempt}-driver-svc",
                              f"{GRPC}:15002", f"{UI}:4040"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)
        if p.poll() is None and socket.socket().connect_ex(("127.0.0.1", GRPC)) == 0:
            try:   # kubectl listens before the pod answers; the driver's REST API shows the tunnel works
                urllib.request.urlopen(f"http://localhost:{UI}/api/v1/applications", timeout=5)
                return p
            except OSError:
                pass
        p.terminate()
        time.sleep(2)
    raise RuntimeError(f"no port forward to connect-{attempt}-driver-svc")


def rest(path):
    base = f"http://localhost:{UI}/api/v1"
    app = json.load(urllib.request.urlopen(f"{base}/applications", timeout=20))[0]["id"]
    return json.load(urllib.request.urlopen(f"{base}/applications/{app}/{path}", timeout=20))


def short(e):
    text = str(e)
    for key in ("PYTHON_VERSION_MISMATCH", "INVALID_HANDLE", "UNAUTHENTICATED", "Unauthenticated"):
        if key in text:
            line = next(l for l in text.splitlines() if key in l)
            return line.strip()[:150]
    return f"{type(e).__name__}: {text.splitlines()[0][:100]}"


start = resource("sparkapp", "connect")["status"]["currentAttemptSummary"]["attemptInfo"]["id"]
pf = forward(start)
url = f"sc://localhost:{GRPC}/;token={TOKEN};user_id=laptop"
anonymous = SparkSession.builder.remote(f"sc://localhost:{GRPC}").create()
try:
    anonymous.range(1).count()
except Exception as e:
    print("without the token:", short(e))
anonymous.client.close()
spark = SparkSession.builder.remote(url).create()
df = (spark.range(0, 2_000_000, numPartitions=8)
      .withColumn("zone", (F.col("id") * 7919 % 263).cast("int"))
      .groupBy("zone").count().orderBy(F.desc("count"), "zone").limit(3))
print("query over Connect, executors in pods:", [tuple(r) for r in df.collect()])
print("server:", spark.version, " client python:", sys.version.split()[0])

host = F.udf(lambda _: __import__("socket").gethostname(), StringType(), useArrow=False)   # see Chapter 30
try:
    spark.range(4, numPartitions=2).select(host("id")).collect()
except Exception as e:
    print("a Python UDF from a 3.12 client:", short(e))

ops = rest("connect/operations")
print("REST connect/operations on the driver pod:",
      [(o["userId"], o["state"], len(o["jobIds"])) for o in sorted(ops, key=lambda o: o["startTimestamp"])])
execs = [(x["id"], x["hostPort"].split(":")[0]) for x in rest("executors") if x["id"] != "driver"]
print("REST executors:", len(execs))

# The driver pod dies. The operator's restart policy starts attempt 1.
print(f"\ndelete pod connect-{start}-driver")
kubectl("delete", "pod", f"connect-{start}-driver", "--wait=false")
t0 = time.time()
seen = []
while time.time() - t0 < 180:
    st = resource("sparkapp", "connect")["status"]
    key = (st["currentAttemptSummary"]["attemptInfo"]["id"], st["currentState"]["currentStateSummary"])
    if key not in seen:
        seen.append(key)
        print(f"  {time.time() - t0:6.1f} s  attempt {key[0]}  {key[1]}")
    if key[0] == start + 1 and key[1] == "RunningHealthy":
        break
    time.sleep(1)
pf.terminate()
pf = forward(start + 1)
# Few, short retries: with the defaults, a client whose server went away can retry for minutes.
spark.client.set_retry_policies([DefaultPolicy(max_retries=4, initial_backoff=500, max_backoff=2000)])
try:
    spark.range(3).count()
except Exception as e:
    print("the old session on the new server:", short(e))
spark2 = SparkSession.builder.remote(url).create()
print(f"a new session on attempt {start + 1}:", spark2.range(3).count(), "rows")
print("pods:", sorted(l.split()[0] for l in pods()[1:] if l.startswith("connect")))
pf.terminate()
sys.stdout.flush()
# No close(): the session that lost its server keeps a retry thread alive, and close() can wait on it.
os._exit(0)
