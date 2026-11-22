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


# ---- Streaming (Part V) ------------------------------------------------------------------
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"   # the Kafka connector for Spark 4.3
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29055")
ROCKSDB = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"


def work(name, clean=True):
    """A scratch folder for one script's inboxes, checkpoints and sinks, under the repo's ignored data/."""
    import shutil
    p = ROOT / "data" / "work" / name
    if clean and p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


def deliver(inbox, name, text, mtime=None):
    """Put a file into a streaming inbox the safe way: write a hidden file, then rename it.

    The file source ignores names that start with '.' or '_', so it never sees half a file.
    mtime (seconds) fixes the order in which the file source picks the files up.
    """
    inbox = Path(inbox)
    hidden = inbox / f".{name}.tmp"
    hidden.write_text(text)
    if mtime is not None:
        os.utime(hidden, (mtime, mtime))
    hidden.rename(inbox / name)
    return inbox / name


def tree(root, skip_crc=True):
    """The files under root, one per line, relative to root (what `find` would show)."""
    root = Path(root)
    out = []
    for p in sorted(root.rglob("*")):
        if p.is_file() and not (skip_crc and p.name.endswith(".crc")):
            out.append(f"{p.relative_to(root)}  ({p.stat().st_size} B)")
    return "\n".join(out)


def error_text(e, width=88, lines=3):
    """The first lines of an exception's message, wrapped for a terminal clip."""
    import textwrap
    msg = " ".join(str(e).split("\n")[0].split())
    if "terminated with exception: " in msg:              # a StreamingQueryException: keep the cause
        msg = "[STREAM_FAILED] ... " + msg.split("terminated with exception: ", 1)[1]
    wrapped = textwrap.wrap(f"{type(e).__name__}: {msg}", width)
    return "\n".join("  " + w for w in wrapped[:lines]) + (" ..." if len(wrapped) > lines else "")
