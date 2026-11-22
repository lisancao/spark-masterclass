"""Helpers shared by every chapter's code: paths, a session, plans and the Spark UI's REST API."""
import contextlib
import io
import json
import os
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = Path(os.environ.get("BOOK_DATA", ROOT / "data"))   # BOOK_DATA lets a second checkout share one download
TAXI = DATA / "nyc-taxi"
TRIPS = TAXI / "yellow"
ZONES = TAXI / "taxi_zone_lookup.csv"
MESSY = ROOT / "code" / "data" / "messy"   # small messy files from SparkTutor (Part IX)
MONTH_START, MONTH_END = "2026-06-01", "2026-07-01"   # the month the book downloads


def session(app, **conf):
    """A local Classic session with the UI on a known port, or a Connect session if SPARK_REMOTE is set."""
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app)
    if not os.environ.get("SPARK_REMOTE"):
        b = b.master(os.environ.get("SPARK_MASTER", "local[*]")).config("spark.ui.port", ui_port())
    for k, v in conf.items():
        b = b.config(k.replace("_", "."), v)
    spark = b.getOrCreate()
    if not os.environ.get("SPARK_REMOTE"):
        spark.sparkContext.setLogLevel("ERROR")
    return spark


def ui_port():
    """The Spark UI port: 4040 unless SPARK_UI_PORT says otherwise (so two runs can share a machine)."""
    return os.environ.get("SPARK_UI_PORT", "4040")


def plan(df, mode="formatted"):
    """The text of df.explain(mode), captured instead of printed. Works on Classic and Connect."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode)
    return buf.getvalue()


def ui_api(path, port=None):
    """GET a path under the Spark UI's monitoring REST API for the first running application."""
    base = f"http://localhost:{port or ui_port()}/api/v1"
    with urllib.request.urlopen(f"{base}/applications") as r:
        app = json.load(r)[0]["id"]
    with urllib.request.urlopen(f"{base}/applications/{app}/{path}") as r:
        return json.load(r)


# Part III: SparkTutor's seeded messy data (small, checked in) and a gitignored scratch folder.
MESSY = ROOT / "code" / "data" / "messy"
WORK = ROOT / "data" / "work"


def show(df, n=20, truncate=False):
    """df.show() with truncate=False by default, which is what every Part III listing wants."""
    df.show(n, truncate=truncate)
