"""Step 2: read lines, not JSON. The text source's schema (one string) can never drift;
try_parse_json and try_variant_get turn each line into typed columns and a reason.

Run from code/:  uv run python ch40/m2_parse.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, work
from clicks import DROPS, parse_events

spark = session("ch40-parse", spark_sql_shuffle_partitions="2", spark_sql_session_timeZone="UTC")

lines = spark.read.text([str(p) for p in DROPS])           # batch first: fast feedback
events = parse_events(lines)
print("Every line gets at most one reason (NULL = a good event):")
events.groupBy("error").count().orderBy("error").show()
print("The rejected lines:")
events.where("error IS NOT NULL").select("error", F.substring("line", 1, 66).alias("line")).show(truncate=False)
print("Drift alarm: keys nobody declared")
(events.where(F.size("unknown_keys") > 0)
 .groupBy(F.col("unknown_keys").cast("string").alias("unknown_keys")).count().show())
print("Tracker 2.0 rows still have a type:",
      events.where("array_contains(unknown_keys, 'app_version') AND type IS NULL").count(), "with type NULL")

print("\nThe same function on a stream: the source schema is one column")
stream = spark.readStream.text(str(DROPS[0].parent))
print("  ", stream.schema.simpleString())
print("  ", parse_events(stream).schema.simpleString()[:120] + " ...")

print("\nparse_json fails the whole job on the first bad line; try_parse_json returns NULL:")
bad = spark.createDataFrame([('{"event_id": "e900", "user_id": "u3", "type": "page_v',)], "value string")
try:
    bad.select(F.parse_json("value")).collect()
except Exception as e:
    import re
    m = re.search(r"\[([A-Z_.]+)\][^\n]*", str(e))
    print("  parse_json:    ", m.group(0)[:110] if m else type(e).__name__)
print("  try_parse_json:", bad.select(F.try_parse_json("value").alias("v")).first().v)
