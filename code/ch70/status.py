"""Chapter 70: print the standalone master's state from its JSON endpoint (http://master:8151/json/)."""
import json
import sys
import urllib.request

port = sys.argv[1] if len(sys.argv) > 1 else "8151"
m = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/json/"))
print(f"master {m['url']}  status {m['status']}  workers alive {m['aliveworkers']}"
      f"  cores {m['cores']}  memory {m['memory']} MiB")
for w in m["workers"]:
    print(f"  worker {w['host']}:{w['port']}  {w['state']:6}  cores {w['cores']}"
          f"  used {w['coresused']}  memory {w['memory']} MiB")
for a in m["activeapps"]:
    print(f"  app {a['id']}  {a['name']}  {a['state']}  cores {a['cores']}")
for d in m.get("activedrivers", []):
    print(f"  driver {d['id']}  {d['state']}  on {d['worker']}  {d['mainclass']}")
