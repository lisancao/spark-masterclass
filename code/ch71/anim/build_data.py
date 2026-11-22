"""Write data.js for the two ch71 animations from the saved output (scheduler_log.txt, anatomy.txt)."""
import json
import re
from pathlib import Path

here = Path(__file__).resolve().parent
out = here.parent / "output"
log = (out / "scheduler_log.txt").read_text()
tasks = [{"stage": int(m[2]), "index": int(m[1]), "executor": m[3], "locality": m[4]}
         for m in re.finditer(r"Starting task (\d+)\.0 in stage (\d+)\.0 \(TID \d+\) \(127\.0\.0\.1,executor (\d), partition \d+, (\w+),", log)]
done = [{"stage": int(m[2]), "index": int(m[1]), "ms": int(m[3])}
        for m in re.finditer(r"Finished task (\d+)\.0 in stage (\d+)\.0 \(TID \d+\) in (\d+) ms", log)]
sched = {"source": "code/ch71/output/scheduler_log.txt", "tasks": tasks, "finished": done,
         "job_ms": float(re.search(r"took ([\d.]+) ms", log)[1])}
(here / "scheduler" / "data.js").write_text("window.DATA = " + json.dumps(sched) + ";\n")

anat = (out / "anatomy.txt").read_text()
sizes = {m[1]: [int(x) for x in m[2].split(", ")]
         for m in re.finditer(r"(shuffle_0_\d_0)\.index\s+\[([\d, ]+)\]", anat)}
map_exec = {}
for m in re.finditer(r"task\s+\d\s+executor (\d)\s+PROCESS_LOCAL\s+\d+ ms\s+rows in\s+[\d,]+\s+shuffle write\s+([\d,]+) B", anat):
    map_exec[int(m[2].replace(",", ""))] = m[1]
maps = [{"name": n, "executor": map_exec[sum(v)], "sizes": v} for n, v in sorted(sizes.items())]
reduce = [{"partition": int(m[1]), "executor": m[2], "local": int(m[3].replace(",", "")), "remote": int(m[4].replace(",", ""))}
          for m in re.finditer(r"task\s+(\d)\s+executor (\d)\s+NODE_LOCAL\s+\d+ ms\s+fetched local ([\d,]+) B remote ([\d,]+) B", anat)]
shuf = {"source": "code/ch71/output/anatomy.txt", "maps": maps, "reduce": reduce}
(here / "shuffle" / "data.js").write_text("window.DATA = " + json.dumps(shuf) + ";\n")
print(json.dumps(sched)[:300]); print(json.dumps(shuf)[:300])
