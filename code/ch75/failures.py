"""Chapter 75, Steps 8-9: five ways a UDF fails, what the driver sees for each, and the settings that help.

Run from code/:  uv run python ch75/failures.py
(The worker's periodic traceback dump goes to the worker's stderr; run with 2>&1 to see it.)
"""
import ctypes, os, re, signal, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("OFF")
spark.conf.set("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
spark.conf.set("spark.sql.execution.pyspark.udf.idleTimeoutSeconds", "3")
spark.conf.set("spark.sql.execution.pyspark.udf.killOnIdleTimeout", "true")

def raise_value(_):
    raise ValueError("bad row 0")
def raise_memory(_):
    raise MemoryError("asked for too much")
def exit_hard(_):
    os._exit(3)
def sigkill(_):
    os.kill(os.getpid(), signal.SIGKILL)
def segfault(_):
    ctypes.string_at(0)
def sleep_long(_):
    time.sleep(30)
    return 0

def classify(exc):
    get = getattr(exc, "getCondition", None)
    if get and get() == "PYTHON_EXCEPTION":
        return "python_exception"
    text = str(exc)
    if "terminated due to idle timeout" in text:
        return "idle_timeout"
    if "Python worker exited unexpectedly (crashed)" in text:
        return "worker_crashed"
    return "other"

def key_text(exc):
    text = str(exc)
    for pat in [r"(Python worker exited unexpectedly \(crashed\):?[^\n]{0,60})",
                r"(Fatal Python error: [^\n]+)", r'(File "[^"]*ctypes[^\n]+)',
                r"(PythonWorkerException: [^\n]{0,80})", r"((ValueError|MemoryError): [^\n]+)",
                r"(java\.io\.EOFException)"]:
        for m in re.finditer(pat, text):
            yield m.group(1).replace(os.path.expanduser("~"), "~")

for f in [raise_value, raise_memory, exit_hard, sigkill, segfault, sleep_long]:
    t = time.perf_counter()
    try:
        spark.range(1).select(F.udf(f, "long", useArrow=False)("id")).collect()
        print(f"{f.__name__}: no error")
    except Exception as e:
        cond = getattr(e, "getCondition", lambda: None)()
        print(f"--- {f.__name__}: {type(e).__name__}, condition {cond}, classified {classify(e)!r}, "
              f"{time.perf_counter() - t:.1f} s")
        seen = []
        for k in key_text(e):
            if k not in seen:
                seen.append(k)
        for k in seen[:4]:
            print(f"    {k[:110]}")

print("\n--- the traceback dump: a worker stuck in time.sleep, dumped every 2 s")
spark.conf.set("spark.sql.execution.pyspark.udf.tracebackDumpIntervalSeconds", "2")
spark.conf.set("spark.sql.execution.pyspark.udf.idleTimeoutSeconds", "5")
sys.stdout.flush()
try:
    spark.range(1).select(F.udf(sleep_long, "long", useArrow=False)("id")).collect()
except Exception as e:
    print("    then:", next(k for k in key_text(e) if "PythonWorkerException" in k)[:100])
