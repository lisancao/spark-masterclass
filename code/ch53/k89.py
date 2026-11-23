"""Shared pieces for Chapter 53: the broker addresses, a session with the Kafka package, publishers."""
import json
import subprocess
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ch48"))

from pyspark.sql import functions as F
from common.book import session, work, error_text, KAFKA_PACKAGE, ZONES as ZONES_CSV   # noqa: F401
from replay import slices, SCHEMA                                     # noqa: F401

PLAIN = "localhost:29116"          # PLAINTEXT listener of ch53/kafka.sh
SASL = "localhost:29117"           # SASL_PLAINTEXT listener (users alice and bob)
HERE = Path(__file__).resolve().parent


def spark_session(app, **conf):
    base = dict(spark_sql_shuffle_partitions="4", spark_sql_session_timeZone="UTC",
                spark_jars_packages=KAFKA_PACKAGE)
    base.update(conf)
    return session(app, **base)


def kafka(*args, check=True):
    """Run ch53/kafka.sh with these arguments and return its stdout."""
    r = subprocess.run([str(HERE / "kafka.sh"), *args], capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError(r.stderr)
    return r.stdout


def trip_rows(files):
    """The JSON lines of these replay slices, as one list of strings."""
    return [l for _, text, _ in files for l in text.splitlines()]


def publish(spark, topic, lines, servers=PLAIN, opts=None, key="PULocationID"):
    """A batch write to Kafka: key = a field of the trip, value = the trip as JSON."""
    df = spark.createDataFrame([(l,) for l in lines], "value string")
    t = F.from_json("value", SCHEMA)
    w = (df.select(t[key].cast("string").alias("key"), "value")
         .write.format("kafka").option("kafka.bootstrap.servers", servers).option("topic", topic))
    for k, v in (opts or {}).items():
        w = w.option(k, v)
    w.save()
    return len(lines)


def end_offsets(topic):
    """{partition: end offset} from the broker's own tool (kafka-get-offsets)."""
    out = kafka("cli", "kafka-get-offsets", "--topic", topic)
    return {int(l.split(":")[1]): int(l.split(":")[2]) for l in out.split()}


def now_ms():
    return int(time.time() * 1000)


def j(obj):
    return json.dumps(obj, separators=(",", ":"))


def err(e, width=84, lines=3):
    """A short, wrapped error: the whole message on one line, and the Java cause for a Py4JJavaError."""
    import re
    import textwrap
    name, msg = type(e).__name__, str(e)
    if name == "Py4JJavaError":
        m = re.search(r"^(?:Caused by: )?([\w.$]+(?:Exception|Error)): (.*)$", msg.split("\n", 1)[1], re.M)
        if m:
            name, msg = m.group(1).rsplit(".", 1)[-1], m.group(2)
    msg = " ".join(msg.split())
    if "terminated with exception: " in msg:
        msg = "[STREAM_FAILED] ... " + msg.split("terminated with exception: ", 1)[1]
    w = textwrap.wrap(f"{name}: {msg}", width)
    return "\n".join("  " + x for x in w[:lines]) + (" ..." if len(w) > lines else "")
