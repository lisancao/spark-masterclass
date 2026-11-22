"""Chapter 45 helpers: a session with the Iceberg runtime and the chapter's catalogs, and file listings.

No released Iceberg supports Spark 4.3 yet. ICEBERG_JAR is a build of apache/iceberg PR #18749
(commit b1eab64, "Spark: Initial support for 4.3.0"), the same jar Part VI uses. notes/ch45.md has its sha256.
"""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import DATA, ROOT, ui_port

ICEBERG_JAR = DATA / "jars" / "iceberg-spark-runtime-4.3_2.13-b1eab64.jar"
WORK = Path(os.environ.get("CH31_WORK", ROOT / "data" / "ch45"))   # catalogs.sh mounts the same folder
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
PACKAGES = "org.xerial:sqlite-jdbc:3.50.3.0"                        # the JDBC catalog keeps its tables in SQLite


def catalog_conf(name, kind):
    """The spark.sql.catalog.<name>.* settings for one catalog of the given kind."""
    p = f"spark.sql.catalog.{name}"
    conf = {p: "org.apache.iceberg.spark.SparkCatalog"}
    if kind == "hadoop":
        conf |= {f"{p}.type": "hadoop", f"{p}.warehouse": f"file://{WORK}/hadoop"}
    elif kind == "jdbc":
        (WORK / "jdbc").mkdir(parents=True, exist_ok=True)
        conf |= {f"{p}.type": "jdbc", f"{p}.uri": f"jdbc:sqlite:{WORK}/jdbc/catalog.db",
                 f"{p}.jdbc.schema-version": "V1", f"{p}.warehouse": f"file://{WORK}/jdbc"}
    elif kind == "hive":
        conf |= {f"{p}.type": "hive", f"{p}.uri": "thrift://localhost:19083",
                 f"{p}.warehouse": f"file://{WORK}/hive"}
    elif kind == "rest":
        conf |= {f"{p}.type": "rest", f"{p}.uri": "http://localhost:18181/api/catalog",
                 f"{p}.warehouse": "taxi", f"{p}.credential": "root:s3cr3t",
                 f"{p}.scope": "PRINCIPAL_ROLE:ALL"}
    return conf


def ice_session(app, catalogs=None, extensions=True, jar=None, **conf):
    """A Classic session with the Iceberg runtime and one catalog per (name, kind) in `catalogs`."""
    catalogs = catalogs if catalogs is not None else {"lake": "hadoop"}
    settings = {"spark.jars": str(jar or ICEBERG_JAR), "spark.sql.session.timeZone": "UTC",
                "spark.sql.shuffle.partitions": "4"}
    if any(k == "jdbc" for k in catalogs.values()):
        settings["spark.jars.packages"] = PACKAGES
    if extensions:
        settings["spark.sql.extensions"] = EXT
    for name, kind in catalogs.items():
        settings |= catalog_conf(name, kind)
    settings |= conf
    # Not common.book.session: it turns "_" into "." in keys, and spark_catalog has an underscore.
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app).master(os.environ.get("SPARK_MASTER", "local[*]"))
    b = b.config("spark.ui.port", ui_port())
    for k, v in settings.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def tidy(text):
    """Shorten paths in text that goes into saved output: WORK becomes $WORK, home becomes ~."""
    return str(text).replace(str(WORK), "$WORK").replace(str(Path.home()), "~")


def tree(path, root=None):
    """Every file under path, relative to root, with its size: what the table looks like on disk."""
    path, root = Path(path), Path(root or path)
    lines = []
    for f in sorted(p for p in path.rglob("*") if p.is_file() and not p.name.endswith(".crc")):
        lines.append(f"{f.stat().st_size:>10,}  {f.relative_to(root)}")
    return "\n".join(lines)


def count_files(path):
    """(data files, metadata files) under a table folder, without .crc files."""
    path = Path(path)
    data = [p for p in (path / "data").rglob("*") if p.is_file() and not p.name.endswith(".crc")]
    meta = [p for p in (path / "metadata").rglob("*") if p.is_file() and not p.name.endswith(".crc")]
    return len(data), len(meta)


def err(e, width=110):
    """The useful first line of a Spark error: the error class or the Java exception, not the Py4J frame."""
    for line in str(e).splitlines():
        line = line.strip().lstrip(": ").strip()
        if line and not line.startswith(("An error occurred", "JVM stacktrace", "==")):
            return tidy(line)[:width]
    return tidy(type(e).__name__)
