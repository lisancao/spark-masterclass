"""Write stream/data.js from code/ch04/output/same_code.txt."""
import json
import re
from pathlib import Path

here = Path(__file__).resolve().parent
t = (here.parent / "output" / "same_code.txt").read_text()
files = re.findall(r"'(2026-06-\d\d\.json)'", t)
batches = [{"id": int(m[1]), "rows": int(m[2].replace(",", ""))} for m in re.finditer(r"batch (\d): +([\d,]+) input rows", t)]
top = [{"zone": int(m[1]), "trips": int(m[2].replace(",", ""))} for m in re.finditer(r"zone +(\d+) +trips +([\d,]+)", t)][:3]
data = {"source": "code/ch04/output/same_code.txt", "files": files, "batches": batches, "top": top,
        "same": "same rows as the batch query: True" in t, "state_rows": int(re.search(r"state rows: (\d+)", t)[1])}
(here / "stream" / "data.js").write_text("window.DATA = " + json.dumps(data) + ";\n")
print(data)
