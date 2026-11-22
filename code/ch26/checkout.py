"""The checkout service's event drops (SparkTutor's messy set): views, cart adds and payments
for seven users on 2026-10-05, with re-sent events, out-of-order events and one very late event."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import ROOT, deliver

DROPS = sorted((ROOT / "code" / "data" / "checkout_events").glob("drop_*.jsonl"))
SCHEMA = "event_id STRING, user STRING, action STRING, page STRING, ts TIMESTAMP, amount DOUBLE"


def drop(inbox, i):
    src = DROPS[i - 1]
    return deliver(inbox, src.name, src.read_text(), mtime=1_000_000 + i)
