"""Chapter 62: shared setup for the SPARK-60206 case study (metric views resolve their source in the
reader's current namespace). A fresh warehouse in a temp dir for every session, so runs never share tables.

PATCHED_CLASSES, if set, is a folder of compiled classes that goes first on the driver class path
(test_fix.sh sets it to the classes built from fix/narrow-fix-v4.3.0.diff).
"""
import os, re, tempfile
from pyspark.sql import SparkSession

DOLLARS = "$" + "$"          # the SQL dollar-quote, built so no shell or tool sees it as a PID


def session(app):
    b = (SparkSession.builder.master("local[2]").appName(app)
         .config("spark.ui.enabled", "false")
         .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="ch62-wh-")))
    if os.environ.get("PATCHED_CLASSES"):
        b = b.config("spark.driver.extraClassPath", os.environ["PATCHED_CLASSES"])
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    return spark


def metric_view_sql(name, source, measure="sum(amount)", filt=None):
    yaml = (f"version: 0.1\nsource: {source}\n" + (f"filter: {filt}\n" if filt else "") +
            "dimensions:\n  - name: region\n    expr: region\n"
            f"measures:\n  - name: total\n    expr: {measure}\n")
    return f"CREATE VIEW {name} WITH METRICS LANGUAGE YAML AS {DOLLARS}\n{yaml}{DOLLARS}"


def answer(spark, sql):
    """The rows of a query as a short string, or the error condition if it fails."""
    try:
        rows = spark.sql(sql).collect()
        return ", ".join("(" + ", ".join(str(v) for v in r) + ")" for r in rows) or "no rows"
    except Exception as e:
        m = re.search(r"\[([A-Z_]+(\.[A-Z_]+)?)\]", str(e))
        return f"error {m.group(1) if m else type(e).__name__}"
