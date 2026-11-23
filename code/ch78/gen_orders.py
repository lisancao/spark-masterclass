"""Chapter 78: write the small order feed that the pipeline reads (seeded, 600 orders in June 2026,
with some messy rows: a bad amount, a missing order_id)."""
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

rng = random.Random(46)
out = Path("/tmp/ch78-pipeline/orders.ndjson")
out.parent.mkdir(parents=True, exist_ok=True)
start = datetime(2026, 6, 1, tzinfo=timezone.utc)
with out.open("w") as f:
    for i in range(600):
        t = start + timedelta(minutes=rng.randrange(0, 5 * 24 * 60))
        row = {"order_id": f"o{i:04d}", "amount": f"{rng.uniform(5, 80):.2f}",
               "event_time": t.strftime("%Y-%m-%dT%H:%M:%S"), "event_ms": int(t.timestamp() * 1000)}
        if i % 97 == 0:
            row["amount"] = "n/a"
        if i % 151 == 0:
            row["order_id"] = None
        f.write(json.dumps(row) + "\n")
print(f"{out}: 600 rows")
