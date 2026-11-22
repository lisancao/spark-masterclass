"""Chapter 54: can spark.addArtifacts deliver a plugin jar? An experiment, not a recipe.

Start the server WITHOUT the plugin jar first:  ch54/server.sh bare
Run from code/:                                  uv run python ch54/artifacts.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession  # noqa: E402
from jplug_client import client_classes  # noqa: E402

JAR = Path(__file__).resolve().parent / "jplug" / "target" / "jplug-0.1.0.jar"


def first_line(e):
    return f"{type(e).__name__}: {str(e).splitlines()[0][:120]}"


with SparkSession.builder.remote("sc://localhost:10300").getOrCreate() as a:
    plan_nodes, _, _ = client_classes()
    try:
        plan_nodes(a.range(3)).collect()
    except Exception as e:
        print("session A, before addArtifacts:", first_line(e))
    a.addArtifacts(str(JAR))
    print("session A, after addArtifacts: ", plan_nodes(a.range(3)).collect())

b = SparkSession.builder.remote("sc://localhost:10300").create()     # a second, separate session
print("session B, never uploaded it:  ", plan_nodes(b.range(3)).collect())
b.stop()
