"""Shorten the log of `airflow dags test` to the lines a reader needs, for the book's output files.

    ch77/af.sh dags test <dag> 2>&1 | python ch77/airflow_log.py [extra regex ...]
"""
import re
import sys

KEEP = [r"^running task", r"new_state=", r"Spark-Submit cmd", r"ZONE_REVENUE",
        r"Fetched \d+ row", r"^(Manhattan|Queens|Brooklyn)\t", r"CONNECT", r"DagRun Finished",
        r"Identified spark", r"Returned value was: \[", r"driver pod", r"phase", *sys.argv[1:]]
LINE = re.compile(r"^\d{4}-\d\d-\d\dT(\d\d:\d\d:\d\d)\.\d+Z \[\w+\s*\] (.*?)(\s+\[[\w.]+\].*)?$")
held = None
for raw in sys.stdin:
    raw = raw.rstrip("\n")
    m = LINE.match(raw)
    if m:
        t, msg, tail = m.group(1), m.group(2).strip(), m.group(3) or ""
    else:
        t, msg, tail = "        ", raw.strip(), ""
    if not msg:
        continue
    if "DAG TEST] running task" in msg:
        msg = "running task " + re.search(r"TaskInstance: [\w.]+\.(\w+)", msg).group(1)
    elif "new_state=" in tail:
        msg = "task state -> " + re.search(r"new_state=(\w+)", tail).group(1)
    elif msg.startswith("DagRun Finished"):
        msg = "DagRun Finished: " + ", ".join(re.findall(r"(?:state|run_duration)=[\w.]+", msg))
    elif "Spark-Submit cmd" in msg:
        msg = re.sub(r"(/tmp|/home)\S*/(ch77|zone_revenue)", r"…/\2", msg)
    msg = re.sub(r"manual__[\dT:.+-]+", "manual__<run>", msg)
    msg = re.sub(r"(/tmp|/home|~)\S*/out/", "…/out/", msg)
    if any(re.search(k, msg + tail) for k in KEEP):
        print(f"{t}  {msg}")
