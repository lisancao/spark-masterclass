"""Chapter 76, Step 4: a SparkCluster. Applies book-cluster.yaml, waits for RunningHealthy, and prints
what the operator made and what the standalone master reports (through the API server proxy).

Run from code/, after ch76/cluster.sh up:  uv run python ch76/run_cluster.py
"""
import json

from kube import HERE, kubectl, wait_state, history

print(kubectl("apply", "-f", str(HERE / "book-cluster.yaml")).strip())
r = wait_state("sparkcluster", "book", lambda s: s == "RunningHealthy")
print("state history of sparkcluster/book:")
print("\n".join(history(r)))
print("what the operator made for it:")
for kind in ("statefulsets", "services", "pods"):
    names = [l.split()[0] for l in kubectl("get", kind, "--no-headers").splitlines() if l.startswith("book-")]
    print(f"  {kind:13} {names}")
master = json.loads(kubectl("get", "--raw",
                            "/api/v1/namespaces/default/services/book-master-svc:8080/proxy/json/"))
print("master /json:", {k: master[k] for k in ("url", "status", "aliveworkers")})
print("workers (host, cores, state):", [(w["host"], w["cores"], w["state"]) for w in master["workers"]])
print("completed applications:", [(a["name"], a["cores"], a["state"]) for a in master["completedapps"]])
