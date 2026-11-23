"""Chapter 38: try each engine's newest release on Spark 4.3.0, and print what stops it.

Each engine needs its own JVM (its jar goes on the driver classpath at launch), so the
script runs itself once per engine in a child process.
Run from code/:  uv run python ch38/blocked_43.py
"""
import subprocess
import sys
import textwrap
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if len(sys.argv) == 3 and sys.argv[1] == "--one":
    from ch38 import engines
    engine = sys.argv[2]
    if engine == "comet-no-shuffle":           # the same jar, without Comet's shuffle manager
        engine = "comet"
        del engines.ENGINES["comet"]["spark.shuffle.manager"]
    if engine == "comet-spark-shuffle":        # the switch that the Comet warning names
        engine = "comet"
        del engines.ENGINES["comet"]["spark.shuffle.manager"]
        engines.ENGINES["comet"]["spark.comet.shuffle.enabled"] = "false"
    try:
        spark = engines.start(engine, **engines.OFFLOAD_FRIENDLY)
    except Exception as e:                    # Py4JJavaError: keep the Java exception line
        java = [l for l in str(e).splitlines() if "Exception:" in l or "Error:" in l]
        print("FAILS AT START:", (java or [str(e)])[0].lstrip(": "))
        sys.exit(0)
    df = engines.queries(spark)["hourly"]
    try:
        df.collect()
    except Exception as e:
        java = [l for l in str(e).splitlines() if "Exception:" in l or "Error:" in l]
        print("FAILS AT QUERY:", (java or [str(e)])[0].lstrip(": ")[:400])
        sys.exit(0)
    e, s, t = engines.native_share(engines.walk(df))
    print(f"STARTS on {spark.version}: hourly ran with {e} engine operators, "
          f"{s} Spark operators, first row {tuple(df.first())}")
    sys.exit(0)

import pyspark
print(f"Spark {pyspark.__version__}, newest release of each engine:\n")
for engine, release in [("gluten", "Gluten 1.7.0 (bin-spark-4.1)"),
                        ("rapids", "RAPIDS 26.10.0 (cuda13)"),
                        ("comet", "Comet 1.2.0 (spark4.2)"),
                        ("auron", "Auron 9.0.0 (compiled for spark-4.2)"),
                        ("comet-no-shuffle", "Comet 1.2.0, no CometShuffleManager"),
                        ("comet-spark-shuffle", "Comet 1.2.0, spark.comet.shuffle.enabled=false"
                         " (not a supported setup)")]:
    r = subprocess.run([sys.executable, __file__, "--one", engine],
                       capture_output=True, text=True)
    line = next((l for l in r.stdout.splitlines() if l.startswith(("FAILS", "STARTS"))),
                f"no result (exit {r.returncode})")
    print(release)
    print(textwrap.fill(line, 88, initial_indent="  ", subsequent_indent="    "))
    if engine == "comet-no-shuffle":
        warn = [l for l in r.stderr.splitlines() if "Comet extension is disabled" in l]
        if warn:
            msg = warn[0].split("WARN ", 1)[1]
            print(textwrap.fill(msg, 88, initial_indent="  log: ", subsequent_indent="    "))
    print()
