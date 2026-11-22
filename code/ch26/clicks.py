"""The clickstream drops (from SparkTutor's messy data set) and helpers to deliver them.

drop_01  tracker 1.0, clean
drop_02  re-sent events, a duplicated line, a truncated line, "ts": "yesterday",
         a missing user, and one event from a phone whose clock is a day ahead
drop_03  tracker 2.0: `type` renamed to `event_type`, a new `app_version`, out-of-order events
drop_04  more 2.0 events, two of them very late
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import ROOT, deliver

DROPS = sorted((ROOT / "code" / "data" / "clickstream").glob("drop_*.jsonl"))
V1_SCHEMA = "event_id STRING, user_id STRING, type STRING, page STRING, ts TIMESTAMP"


def drop(inbox, i):
    """Deliver drop i (1-based) into inbox, with an mtime that keeps the drops in order."""
    src = DROPS[i - 1]
    return deliver(inbox, src.name, src.read_text(), mtime=1_000_000 + i)


# ---- Parsing that never fails (Step 2) ---------------------------------------------------
from pyspark.sql import functions as F

KNOWN_KEYS = ["event_id", "user_id", "type", "event_type", "page", "ts"]
CLOCK_SKEW = "5 minutes"
NOW = "2026-10-05 10:00:00"      # the book's fixed "now"; a real job passes F.current_timestamp()


def parse_events(lines, now=None, reject_future=True):
    """One string column `value` in; typed columns, unknown keys and one error reason out."""
    now = now if now is not None else F.lit(NOW).cast("timestamp")
    v = F.try_parse_json("value")
    parsed = lines.select(F.col("value").alias("line"), v.alias("v")).select(
        "line",
        F.try_variant_get("v", "$.event_id", "string").alias("event_id"),
        F.try_variant_get("v", "$.user_id", "string").alias("user_id"),
        F.coalesce(F.try_variant_get("v", "$.type", "string"),          # tracker 1.0
                   F.try_variant_get("v", "$.event_type", "string")).alias("type"),   # 2.0
        F.try_variant_get("v", "$.page", "string").alias("page"),
        F.try_to_timestamp(F.try_variant_get("v", "$.ts", "string")).alias("ts"),
        F.array_except(F.expr("json_object_keys(line)"),
                       F.array(*[F.lit(k) for k in KNOWN_KEYS])).alias("unknown_keys"),
        F.col("v").isNull().alias("_bad_json"))
    error = (F.when(F.col("_bad_json"), "bad_json")
             .when(F.col("event_id").isNull(), "missing_event_id")
             .when(F.col("user_id").isNull(), "missing_user")
             .when(F.col("ts").isNull(), "bad_ts"))
    if reject_future:
        error = error.when(F.col("ts") > now + F.expr(f"INTERVAL {CLOCK_SKEW}"), "future_ts")
    return parsed.withColumn("error", error).drop("_bad_json")
