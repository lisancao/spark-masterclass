"""Chapter 47, open finding: a Python UDF over a Comet scan, on Spark 4.2.0 with Comet 1.2.0.

Writes a small Parquet file, then runs one Python UDF over it with Comet, two times:
with spark.sql.execution.arrow.pythonUDF.columnarInput.enabled at its default (true,
new in Spark 4.2.0), and with it set to false. Each run gets 120 s.
Run from code/:  ~/spark-masterclass/data/ch47/venv-4.2.0/bin/python ch47/repro_comet_udf.py
"""
import os
import subprocess
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if len(sys.argv) == 3 and sys.argv[1] == "--one":
    import threading
    from pyspark.sql import functions as F
    from ch47.engines import start
    path = sys.argv[2]
    columnar = os.environ["COLUMNAR_INPUT"]
    spark = start("comet", **{"spark.sql.execution.arrow.pythonUDF.columnarInput.enabled": columnar})
    spark.range(1_000_000).withColumn("x", F.col("id") % 7).write.mode("overwrite").parquet(path)
    df = spark.read.parquet(path).select(F.udf(lambda x: x + 1, "long")("x").alias("y"))
    df = df.agg(F.sum("y"))
    done = threading.Event()
    out = {}
    threading.Thread(target=lambda: (out.update(r=df.collect()), done.set()), daemon=True).start()
    ok = done.wait(120)
    plan = df._jdf.queryExecution().executedPlan().toString()
    udf_op = [l.strip() for l in plan.splitlines() if "EvalPython" in l]
    print(f"columnarInput.enabled={columnar}: "
          f"{'sum = ' + str(out['r'][0][0]) if ok else 'NO RESULT after 120 s'}")
    print(f"  {udf_op[0][:80] if udf_op else '(no EvalPython operator)'}")
    sys.stdout.flush()
    os._exit(0)

import pyspark
print(f"Spark {pyspark.__version__}, Comet 1.2.0, one Python UDF over a Comet Parquet scan")
with tempfile.TemporaryDirectory() as d:
    for columnar in ("true", "false"):
        r = subprocess.run([sys.executable, __file__, "--one", f"{d}/t.parquet"],
                           env={**os.environ, "COLUMNAR_INPUT": columnar},
                           capture_output=True, text=True, timeout=400)
        print("\n".join(l for l in r.stdout.splitlines() if l.strip()) or r.stderr[-400:])
