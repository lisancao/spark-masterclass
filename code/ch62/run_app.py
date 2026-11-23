"""Chapter 62, Steps 2 and 3: run a SparkApplication and follow it through the operator's states.

Polls the resource every second and prints each new (attempt, state), with the seconds since the
apply, the driver pods it saw, and the important lines of each driver log.

Run from code/, after ch62/cluster.sh up:
  uv run python ch62/run_app.py zones-app.yaml
  uv run python ch62/run_app.py failing-app.yaml
"""
import sys
import time

from kube import HERE, kubectl, resource, pods

KEEP = ("Executor added", "top zones", "rows by", "driver pod", "rows:", "RuntimeError")
manifest = HERE / sys.argv[1]
name = next(l.split(":")[1].strip() for l in manifest.read_text().splitlines() if l.strip().startswith("name:"))
kubectl("delete", "sparkapp", name, "--ignore-not-found", "--wait=true")
print(kubectl("apply", "-f", str(manifest)).strip())
t0, seen, logs, executors = time.time(), set(), {}, set()
while time.time() - t0 < 400:
    st = (resource("sparkapp", name) or {}).get("status", {})
    state = st.get("currentState", {}).get("currentStateSummary")
    attempt = st.get("currentAttemptSummary", {}).get("attemptInfo", {}).get("id")
    if state and (attempt, state) not in seen:
        seen.add((attempt, state))
        print(f"  {time.time() - t0:6.1f} s  attempt {attempt}  {state:34} {st['currentState'].get('message', '')[:44]}")
    for line in pods()[1:]:
        pod, phase = line.split()[:2]
        if pod.startswith(name) and "driver" in pod and phase in ("Succeeded", "Failed") and pod not in logs:
            logs[pod] = [l for l in kubectl("logs", pod, check=False).splitlines() if l.startswith(KEEP)]
        if pod.startswith(name) and "exec" in pod:
            executors.add(pod)
    if state in ("ResourceReleased", "TerminatedWithoutReleaseResources"):
        break
    time.sleep(1)
print("executor pods seen:", sorted(executors))
for pod, lines in sorted(logs.items()):
    print(f"log of {pod}:")
    print("\n".join("  " + l[:110] for l in lines))
