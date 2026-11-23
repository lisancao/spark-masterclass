"""Chapter 75, Step 5: a Python UDF fails on an executor. What reaches the driver, under each setting.

Run from code/:  uv run python ch75/tracebacks.py      (SPARK_REMOTE=sc://... for Connect)
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F  # noqa: E402
from common.book import session  # noqa: E402

spark = session("ch75-tracebacks", spark_ui_showConsoleProgress="false")
spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")   # the pickled UDF: one stack, one row
mode = "Connect" if os.environ.get("SPARK_REMOTE") else "Classic"


@F.udf("double")
def tip_share(tip, fare):
    rate = tip / fare            # fare = 0.0 raises ZeroDivisionError
    return round(rate, 3)


df = spark.createDataFrame([(3.0, 12.5), (1.0, 0.0)], "tip double, fare double").select(tip_share("tip", "fare"))


HERE = str(Path(__file__).resolve().parent) + "/"


def show(title, query=None, **conf):
    for k, v in conf.items():
        spark.conf.set(k, v)
    try:
        (query or df).collect()
    except Exception as e:
        text = str(e).replace(HERE, "")
        lines = text.splitlines()
        jvm = sum(1 for line in lines if line.strip().startswith("at "))
        print(f"\n== {title}")
        print(f"   class {type(e).__module__}.{type(e).__name__}, condition {e.getCondition()}, "
              f"sqlState {e.getSqlState()}")
        print(f"   {len(lines)} lines in str(e), {jvm} JVM 'at ...' frames, "
              f"'File \"' lines: {sum(1 for line in lines if 'File \"' in line)}")
        return lines
    finally:
        for k in conf:
            spark.conf.unset(k)


print(f"{mode}: a UDF divides by zero in a task")
lines = show("defaults (simplifiedTraceback on, jvmStacktrace off)")
print("   " + "\n   ".join(line[:92] for line in lines[:12]))
lines = show("simplifiedTraceback.enabled=false",
             **{"spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled": "false"})
print("   Python frames: " + ", ".join(line.split("/")[-1].strip()[:40] for line in lines if 'File "' in line)[:400])
show("jvmStacktrace.enabled=true", **{"spark.sql.pyspark.jvmStacktrace.enabled": "true"})
sql = spark.sql("SELECT 1 / 0 AS x")
lines = show("a JVM error (SELECT 1 / 0), defaults", query=sql)
print("   " + lines[0][:92])
lines = show("a JVM error (SELECT 1 / 0), jvmStacktrace.enabled=true", query=sql,
             **{"spark.sql.pyspark.jvmStacktrace.enabled": "true"})
print("   " + "\n   ".join(line[:92] for line in lines if "JVM stacktrace" in line or "DivideByZero" in line
                             or line.strip().startswith("at org.apache.spark.sql.errors"))[:600])
if mode == "Connect":
    show("a JVM error, Connect, spark.sql.connect.serverStacktrace.enabled=false", query=sql,
         **{"spark.sql.connect.serverStacktrace.enabled": "false"})
lines = show("tracebackWithLocals.enabled=true (new in 4.3)",
             **{"spark.sql.execution.pyspark.udf.tracebackWithLocals.enabled": "true"})
print("   " + "\n   ".join(line[:92] for line in lines if line.strip().startswith(("tip", "fare", "rate", "File"))))
spark.stop()
