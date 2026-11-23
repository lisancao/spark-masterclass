"""Chapter 81: print the AUTH environment variables of a pod spec (JSON on stdin), with values cut."""
import json
import sys

for e in json.load(sys.stdin)["spec"]["containers"][0].get("env", []):
    if "AUTH" in e["name"]:
        v = e.get("value")
        shown = f"{v[:6]}... ({len(v)} chars, in clear text in the pod spec)" if v else e.get("valueFrom")
        print(f"  {e['name']} = {shown}")
