"""Chapter 63: what the Spark Connect server recorded for the Airflow task, from its REST API (new in 4.3).

    SPARK_UI_PORT=4064 uv run python ch63/connect_rest.py

It reads /api/v1/applications/<app>/connect/sessions and .../connect/operations on the Connect server's UI.
"""
import datetime as dt
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.book import ui_api  # noqa: E402


def ts(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%H:%M:%S") if ms else "-"


print("# GET .../connect/sessions")
for s in ui_api("connect/sessions"):
    print(f"session {s['sessionId'][:8]}  user={s['userId']}  started {ts(s['startTimestamp'])}  "
          f"finished {ts(s.get('finishTimestamp'))}  operations={s['totalExecution']}")

print("\n# GET .../connect/operations")
for op in ui_api("connect/operations"):
    site = re.findall(r'call_site: "([^"]+)"', op["statement"])
    site = os.path.relpath(site[0], os.path.join(os.path.dirname(__file__), "..")) if site else "-"
    print(f"operation {op['operationId'][:8]}  session {op['sessionId'][:8]}  state={op['state']}  "
          f"jobs={','.join(op['jobIds'])}  sql={','.join(op['sqlExecIds'])}")
    print(f"  call site: {site}")
