"""Part VI helpers: a local session with the Iceberg runtime for Spark 4.3 and a Hadoop catalog.

The runtime is an unreleased build of apache/iceberg PR #18749 (commit b1eab64), because no released
Iceberg version supports Spark 4.3 yet. notes/part06.md records how it was built and its sha256.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import DATA, session

ICEBERG_JAR = DATA / "jars" / "iceberg-spark-runtime-4.3_2.13-b1eab64.jar"
WORK = DATA / "work" / "part06"


def lake_session(app, warehouse, **conf):
    """A Classic session with an Iceberg catalog called `lake` at `warehouse` (a local folder)."""
    return session(app, **{
        "spark.jars": str(ICEBERG_JAR),
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lake.type": "hadoop",
        "spark.sql.catalog.lake.warehouse": f"file://{warehouse}",
        "spark.sql.shuffle.partitions": "4",
        "spark.sql.session.timeZone": "UTC",
        **conf,
    })


def tidy(text):
    """Remove the home directory from text that goes into saved output."""
    return text.replace(str(Path.home()), "~")


if __name__ == "__main__":
    # python common/lake.py <warehouse> <namespace>: create a namespace before a pipeline uses it,
    # because a spec's `database` must already exist when the CLI creates the dataflow graph.
    import sys
    s = lake_session("create-namespace", sys.argv[1])
    s.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{sys.argv[2]}")
    print(f"namespace lake.{sys.argv[2]} is ready")
