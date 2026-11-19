"""Helpers shared by every chapter's code: paths, a session, plans and the Spark UI's REST API."""
import contextlib
import io
import json
import os
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
TAXI = DATA / "nyc-taxi"
TRIPS = TAXI / "yellow"
ZONES = TAXI / "taxi_zone_lookup.csv"
MONTH_START, MONTH_END = "2026-06-01", "2026-07-01"   # the month the book downloads


def session(app, **conf):
    """A local Classic session with the UI on a known port, or a Connect session if SPARK_REMOTE is set."""
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app)
    if not os.environ.get("SPARK_REMOTE"):
        b = b.master(os.environ.get("SPARK_MASTER", "local[*]")).config("spark.ui.port", "4040")
    for k, v in conf.items():
        b = b.config(k.replace("_", "."), v)
    spark = b.getOrCreate()
    if not os.environ.get("SPARK_REMOTE"):
        spark.sparkContext.setLogLevel("ERROR")
    return spark


def plan(df, mode="formatted"):
    """The text of df.explain(mode), captured instead of printed. Works on Classic and Connect."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode)
    return buf.getvalue()


def ui_api(path, port=4040):
    """GET a path under the Spark UI's monitoring REST API for the first running application."""
    base = f"http://localhost:{port}/api/v1"
    with urllib.request.urlopen(f"{base}/applications") as r:
        app = json.load(r)[0]["id"]
    with urllib.request.urlopen(f"{base}/applications/{app}/{path}") as r:
        return json.load(r)
