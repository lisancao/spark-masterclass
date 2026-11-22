"""Chapter 6: the same nanosecond file over Spark Connect (server from ch06/connect_server.sh).

Run from code/:  SPARK_REMOTE=sc://localhost:15052 uv run python ch06/nanos_connect.py
"""
import re
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa
import pyarrow.parquet as pq
from common.book import session

spark = session("ch06-nanos-connect")
tmp = Path(tempfile.mkdtemp(prefix="ch06-nanos-"))
pq.write_table(pa.table({"event_ts": pa.array([1780308000123456789, 1780308000123456790], type=pa.timestamp("ns"))}),
               tmp / "events.parquet")
spark.conf.set("spark.sql.timestampNanosTypes.enabled", "true")
df = spark.read.parquet(str(tmp))


def attempt(f):
    try:
        return f()
    except Exception as e:
        lines = [m.group(0) for l in str(e).splitlines() if (m := re.search(r"\[[A-Z_.]+\].*", l))]
        return "ERROR " + (lines[0] if lines else str(e).splitlines()[0])[:80]


print("Spark Connect client", spark.version)
for label, f in [("df.schema", lambda: df.schema), ("collect()", lambda: df.collect()),
                 ("toPandas()", lambda: df.toPandas().event_ts.tolist()), ("toArrow()", lambda: df.toArrow().schema)]:
    print(f"  {label:<12} {str(attempt(f))[:100]}")
