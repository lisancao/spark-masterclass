"""Summarize the spark-connect-matrix runs saved in output/matrix/ (harness from ~/spark-connect-matrix)."""
import json
from collections import Counter
from pathlib import Path

for f in sorted((Path(__file__).parent / "output" / "matrix").glob("*.json")):
    run = json.loads(f.read_text())
    c = Counter(r["status"] for r in run["results"])
    print(f"{run['client']} {run['client_version']} on {run['server']}: "
          f"{c['pass']} pass, {c['fail']} fail, {c["skip"]} skipped")
    for r in run["results"]:
        if r["status"] != "pass":
            print(f"  {r['status']:<5} {r['id']}: {str(r['detail'])[:70]}")
