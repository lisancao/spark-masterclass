"""After ch83/run.sh app: the operations of the app in the server's REST API, found by its tag."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.book import ui_api  # noqa: E402

ops = [o for o in ui_api("connect/operations", port=4146) if "taxi-app" in o.get("sparkSessionTags", [])]
print(f"operations with the tag taxi-app: {len(ops)}")
for o in ops[:3]:
    print(json.dumps({k: o[k] for k in ("userId", "sparkSessionTags", "state", "statement")}, default=str)[:150])
