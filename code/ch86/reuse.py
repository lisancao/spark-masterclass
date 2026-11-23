"""Chapter 86, new in 4.3: spark.local.connect.reuse keeps one local Connect server for many runs.

Run from code/:  uv run python ch86/reuse.py
The script starts two child processes. The first one starts a persistent server with the
plugin jar; the second one finds it through the discovery file and connects to it.
Then the script stops the server with python -m pyspark.sql.connect.local_server --stop.
"""
import json, os, subprocess, sys, time, warnings
from pathlib import Path

HERE = Path(__file__).resolve().parent
JAR = HERE / "jplug" / "target" / "jplug-0.1.0.jar"
STATE = HERE.parents[1] / "data" / "ch86-reuse"
ENV = dict(os.environ, SPARK_LOCAL_CONNECT_DISCOVERY=str(STATE / "discovery.json"))


def child(run):
    sys.path.insert(0, str(HERE))
    from pyspark.sql import SparkSession
    from jplug_client import client_classes

    warnings.filterwarnings("ignore", message="Failed to set")
    t = time.perf_counter()
    spark = (SparkSession.builder.remote("local[2]")
             .config("spark.local.connect.reuse", "true")
             .config("spark.local.connect.server.port", "10301")
             .config("spark.jars", str(JAR))
             .config("spark.connect.extensions.relation.classes", "jplug.PlanNodesPlugin")
             .config("spark.ui.port", "4131")
             .getOrCreate())
    ready = time.perf_counter() - t
    plan_nodes, _, _ = client_classes()
    nodes = [r.node for r in plan_nodes(spark.range(3).filter("id > 0")).collect()]
    print(f"run {run}: session ready in {ready:4.1f} s | {spark.client._builder.url.netloc}"
          f" | session {spark.session_id[:8]} | plan_nodes -> {nodes}")
    spark.stop()


if len(sys.argv) > 1:
    child(sys.argv[1])
    sys.exit()

STATE.mkdir(parents=True, exist_ok=True)
for run in (1, 2):
    out = subprocess.run([sys.executable, __file__, str(run)], env=ENV, capture_output=True, text=True)
    print(out.stdout.strip() or out.stderr.strip().splitlines()[-1])
    if run == 1:
        d = json.loads((STATE / "discovery.json").read_text())
        print("discovery file keys:", sorted(d), "| port", d["port"], "| Spark", d["spark_version"])

stop = subprocess.run([sys.executable, "-m", "pyspark.sql.connect.local_server", "--stop"],
                      env=ENV, capture_output=True, text=True)
print("--stop:", (stop.stdout + stop.stderr).strip() or f"exit {stop.returncode}")
