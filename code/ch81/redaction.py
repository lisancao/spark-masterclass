"""Chapter 81, Step 6: what Spark redacts in the UI, the event log and SQL output, and what it does not.
"""
import glob
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ZONES, plan, session, ui_api, work  # noqa: E402

W = work("ch81-redaction")
spark = session(
    "ch81-redaction",
    **{
        "spark.hadoop.fs.s3a.secret.key": "s3-SECRET-1234",       # key matches "secret"
        "spark.book.api.token": "tok-SECRET-5678",                 # key matches "token"
        "spark.book.endpoint": "https://svc.example/?password=pw-SECRET-9",  # value matches "password"
        "spark.book.region": "ca-central-1",                       # no match
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": str(W),
        "spark.eventLog.compress": "false",
    },
)
keys = ["spark.hadoop.fs.s3a.secret.key", "spark.book.api.token", "spark.book.endpoint", "spark.book.region"]

print("1. REST /environment (the Environment tab reads the same store)")
props = dict(ui_api("environment")["sparkProperties"])
for k in keys:
    print(f"   {k:<32} {props[k]}")

print("2. the event log, SparkListenerEnvironmentUpdate")
spark.range(1).count()
log = sorted(glob.glob(f"{W}/*/events_*"))[0]   # a rolling event log folder (Chapter 61)
for line in open(log):
    e = json.loads(line)
    if e["Event"] == "SparkListenerEnvironmentUpdate":
        for k in keys:
            print(f"   {k:<32} {e['Spark Properties'][k]}")

print("3. the application itself still reads the real values")
for k in keys[:2]:
    print(f"   spark.conf.get({k!r}) = {spark.conf.get(k)}")

print("4. table options in DESCRIBE TABLE EXTENDED (spark.sql.redaction.options.regex)")
spark.sql(f"""CREATE TABLE zones_csv USING csv
  OPTIONS (path '{ZONES}', header 'true', password 'pw-SECRET-1', url 'jdbc:x://db/?user=a&password=b')""")
for r in spark.sql("DESCRIBE TABLE EXTENDED zones_csv").collect():
    if r.col_name in ("Storage Properties", "Location"):
        print(f"   {r.col_name}: {r.data_type.replace(str(ZONES.parent), '$TAXI')}")

print("5. plan strings with spark.sql.redaction.string.regex (a runtime SQL config)")
df = spark.read.option("header", True).csv(str(ZONES)).where("Zone = 'acct-SECRET-42'")
before = [l for l in plan(df, "simple").splitlines() if "acct" in l][0].strip()
spark.conf.set("spark.sql.redaction.string.regex", "acct-SECRET-[0-9]+")
after = [l for l in plan(df, "simple").splitlines() if "Filter" in l][0].strip()
print(f"   before: {before[:88]}")
print(f"   after:  {after[:88]}")
spark.sql("DROP TABLE zones_csv")
spark.stop()
