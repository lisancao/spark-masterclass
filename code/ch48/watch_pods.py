"""Print each change of pod phase in the spark-jobs namespace of the kind cluster "ch48", with the time.

    python ch48/watch_pods.py <seconds> [--until-done]

Pod names are made short: "etl-a driver", "etl-a exec-1", and "etl-a placeholder (executor)" for the
placeholder pods that YuniKorn makes for a gang. At the end, it prints the YuniKorn state of each
application, from the scheduler's REST API (port-forwarded to localhost:19048 by yunikorn.sh). YuniKorn counts vcore in thousandths of a core..
"""
import json
import re
import subprocess
import sys
import time
import urllib.request

seconds = float(sys.argv[1])
until_done = "--until-done" in sys.argv


def pods():
    out = subprocess.run(["kubectl", "--context", "kind-ch48", "-n", "spark-jobs", "get", "pods", "-o", "json"],
                         capture_output=True, text=True, check=True).stdout
    return json.loads(out)["items"]


def names(items):
    """Map YuniKorn application IDs (spark-<hex>) to the app name in the driver pod's name."""
    apps = {}
    for p in items:
        lab = p["metadata"].get("labels", {})
        if lab.get("spark-role") == "driver":
            apps[lab["spark-app-selector"]] = re.sub(r"-[0-9a-f]{16}-driver$", "", p["metadata"]["name"])
    return apps


def short(p, apps):
    meta = p["metadata"]
    lab, ann = meta.get("labels", {}), meta.get("annotations", {})
    app = apps.get(lab.get("spark-app-selector") or lab.get("yunikorn.apache.org/app-id"), "?")
    if meta["name"].startswith("tg-"):
        return f"{app} placeholder ({ann.get('yunikorn.apache.org/task-group-name')})"
    role = lab.get("spark-role")
    if role == "driver":
        return f"{app} driver"
    return f"{app} exec-{lab.get('spark-exec-id')}"


def state(p):
    phase = p["status"].get("phase")
    for cs in p["status"].get("containerStatuses", []):
        if "terminated" in cs["state"]:
            return "Completed" if cs["state"]["terminated"]["exitCode"] == 0 else "Failed"
    if p["metadata"].get("deletionTimestamp"):
        return "Terminating"
    return phase


seen, t0, apps = {}, time.time(), {}
while time.time() - t0 < seconds:
    items = pods()
    apps.update(names(items))
    now = {}
    for p in items:
        if p["metadata"]["name"].startswith("tg-") or p["metadata"].get("labels", {}).get("spark-role"):
            now[p["metadata"]["uid"]] = (short(p, apps), state(p))
    for uid, (name, st) in sorted(now.items(), key=lambda kv: kv[1][0]):
        if seen.get(uid) != (name, st):
            print(f"t+{time.time() - t0:4.0f}s  {name:<30} {st}", flush=True)
    for uid in seen.keys() - now.keys():
        print(f"t+{time.time() - t0:4.0f}s  {seen[uid][0]:<30} deleted", flush=True)
    seen = now
    drivers = [st for name, st in now.values() if name.endswith(" driver")]
    if until_done and drivers and all(st in ("Completed", "Failed") for st in drivers):
        break
    time.sleep(1)

try:
    url = "http://localhost:19048/ws/v1/partition/default/queue/root.etl/applications"
    for a in json.load(urllib.request.urlopen(url, timeout=5)):
        used, pend = a.get("usedResource") or {}, a.get("pendingResource") or {}
        print(f"yunikorn  {apps.get(a['applicationID'], a['applicationID']):<6} {a['applicationState']:<10} "
              f"cores used {used.get('vcore', 0) / 1000:g}, cores waiting {pend.get('vcore', 0) / 1000:g}")
    q = json.load(urllib.request.urlopen("http://localhost:19048/ws/v1/partition/default/queue/root.etl", timeout=5))
    used, top = q.get("allocatedResource") or {}, q.get("maxResource") or {}
    print(f"yunikorn  queue root.etl  cores used {used.get('vcore', 0) / 1000:g} of max {top.get('vcore', 0) / 1000:g}")
except OSError as e:
    print("yunikorn REST API not reachable:", e)
