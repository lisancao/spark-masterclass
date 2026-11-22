"""Chapter 45 helpers: kubectl against the kind cluster "part08", and the status of a Spark resource."""
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
CTX = "kind-part08"


def kubectl(*args, check=True):
    r = subprocess.run(["kubectl", "--context", CTX, *args], capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError(r.stderr)
    return r.stdout


def resource(kind, name):
    out = kubectl("get", kind, name, "-o", "json", check=False)
    return json.loads(out) if out else None


def wait_state(kind, name, done, timeout=300):
    """Poll .status.currentState until done(state) is true. Returns the resource."""
    end = time.time() + timeout
    while time.time() < end:
        r = resource(kind, name)
        state = (r or {}).get("status", {}).get("currentState", {}).get("currentStateSummary")
        if state and done(state):
            return r
        time.sleep(2)
    raise TimeoutError(f"{kind}/{name} did not reach the state")


def history(r):
    """One line for each state transition, with seconds since the first one."""
    h = r["status"]["stateTransitionHistory"]
    rows = [h[k] for k in sorted(h, key=int)]
    t0 = datetime.fromisoformat(rows[0]["lastTransitionTime"][:26].rstrip("Z"))
    out = []
    for row in rows:
        t = datetime.fromisoformat(row["lastTransitionTime"][:26].rstrip("Z"))
        out.append(f"  {(t - t0).total_seconds():6.1f} s  {row['currentStateSummary']:36} {row.get('message', '')[:50]}")
    return out


def pods(selector=None):
    args = ["get", "pods", "-o", "custom-columns=NAME:.metadata.name,STATUS:.status.phase,NODE:.spec.nodeName"]
    if selector:
        args += ["-l", selector]
    return [l for l in kubectl(*args).splitlines()]
