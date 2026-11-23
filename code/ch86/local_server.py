"""Chapter 86: remote("local[2]") starts a Connect server inside this Python process.

No server.sh needed. The server listens on the default Connect port, 15002, and the
client connects to sc://localhost (also 15002): check that nothing else owns that port.
Run from code/:  uv run python ch86/local_server.py
"""
import socket, sys, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession  # noqa: E402
from jplug_client import client_classes  # noqa: E402

JAR = Path(__file__).resolve().parent / "jplug" / "target" / "jplug-0.1.0.jar"

with socket.socket() as s:
    busy = s.connect_ex(("127.0.0.1", 15002)) == 0
print("something already listens on 15002:", busy)
if busy:
    sys.exit("stop the other server first: the client would talk to it, not to ours")

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    spark = (SparkSession.builder.remote("local[2]")
             .config("spark.jars", str(JAR))
             .config("spark.connect.extensions.relation.classes", "jplug.PlanNodesPlugin")
             .config("spark.ui.port", "4060")
             .getOrCreate())
print(type(spark).__module__, "| server runs Spark", spark.version)
seen = []
for w in caught:
    msg = str(w.message)
    if msg.startswith("Failed to set"):
        line = msg.split(" due to ")[0].split(" to Some(")[0] + " due to " + msg.split(" due to ")[1].split("]")[0] + "]"
        if line not in seen:
            seen.append(line)
            print("warning:", line)

plan_nodes, _, _ = client_classes()
print(plan_nodes(spark.range(3).filter("id > 0")).select("node", "parent").collect())
spark.stop()
