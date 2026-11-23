"""The _SUCCESS file that each committer wrote (run committers.py first)."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s3 import mc  # noqa: E402

for name in ("file-v1", "magic"):
    body = mc("mc cat", f"m/book/commit/{name}/run3/_SUCCESS")
    print(f"== {name}: _SUCCESS is {len(body.encode())} bytes")
    if not body:
        continue
    d = json.loads(body)
    for k in ("name", "committer", "jobId", "jobIdSource", "description"):
        print(f"  {k}: {str(d.get(k))[:70]}")
    print(f"  filenames: {len(d['filenames'])}, the first: {d['filenames'][0][:62]}")
    m = d.get("metrics", {})
    print(f"  metrics: {len(m)} counters, for example "
          f"object_copy_requests={m.get('object_copy_requests')}, "
          f"committer_magic_files_created={m.get('committer_magic_files_created')}")
    print(f"  diagnostics: {sorted(d.get('diagnostics', {}))[:4]}")
