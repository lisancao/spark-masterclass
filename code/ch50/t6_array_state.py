"""Open finding (notes/v3-streaming.md): applyInPandasWithState with an ARRAY field in the state schema.

Run from code/:  uv run python ch50/t6_array_state.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
from pyspark.sql.streaming.state import GroupStateTimeout
from common.book import session, work, error_text

spark = session("ch50-array-state", spark_ui_showConsoleProgress="false")
w = work("ch50/t6_array")
(w / "in").mkdir()
(w / "in" / "a.json").write_text('{"k": 0}\n{"k": 1}\n{"k": 0}\n')


def f(key, pdfs, state):
    n = sum(len(p) for p in pdfs)
    state.update((n,) if STATE == "n LONG" else ([str(n)],))
    yield pd.DataFrame({"k": [key[0]], "n": [n]})


for STATE in ("n LONG", "ids ARRAY<STRING>"):
    df = (spark.readStream.schema("k LONG").json(str(w / "in")).groupBy("k")
          .applyInPandasWithState(f, "k LONG, n LONG", STATE, "update", GroupStateTimeout.NoTimeout))
    q = (df.writeStream.format("noop").outputMode("update")
         .option("checkpointLocation", str(w / STATE.split()[0])).trigger(availableNow=True).start())
    try:
        q.awaitTermination()
        print(f"state schema {STATE!r}: ran, batches {len(q.recentProgress)}")
    except Exception as e:
        print(f"state schema {STATE!r}:")
        print(error_text(e, 88, 1))
        print("  ...", [l.strip() for l in str(e).splitlines() if l.strip().startswith("KeyError")][0])
    finally:
        q.stop()
spark.stop()
