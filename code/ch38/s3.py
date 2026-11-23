"""Shared code for Chapter 38: a session that talks S3A to the local MinIO, and a request trace.

MinIO runs in Docker (ch38/minio.sh up). The keys are test keys for that container only.
"""
import collections
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session  # noqa: E402

ENDPOINT = "http://localhost:19055"
ACCESS, SECRET = "bookadmin", "bookadmin-secret"
CLOUD = "org.apache.spark:spark-hadoop-cloud_2.13:4.3.0"   # the hadoop-cloud module for Spark 4.3.0
MC = "minio/mc:RELEASE.2025-04-08T15-39-49Z"

S3A = {
    "spark.jars.packages": CLOUD,
    "spark.hadoop.fs.s3a.endpoint": ENDPOINT,
    "spark.hadoop.fs.s3a.path.style.access": "true",      # MinIO serves buckets as paths, not host names
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
    "spark.hadoop.fs.s3a.access.key": ACCESS,
    "spark.hadoop.fs.s3a.secret.key": SECRET,
}

MANIFEST = "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory"

# The committers that the chapter compares. Each one is a set of extra configurations.
FILE = {"spark.hadoop.fs.s3a.committer.magic.enabled": "false"}   # back to FileOutputCommitter
COMMITTERS = {
    "file-v1": {**FILE, "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "1"},
    "file-v2": {**FILE, "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"},
    "magic": {},                                                   # the default with hadoop-cloud
    "directory": {"spark.hadoop.fs.s3a.committer.name": "directory"},
    "partitioned": {"spark.hadoop.fs.s3a.committer.name": "partitioned"},
}


def s3_session(app, extra=None, master="local[4]"):
    """A classic local session with the hadoop-cloud module and S3A pointed at MinIO."""
    import os
    os.environ.setdefault("SPARK_MASTER", master)
    conf = {**S3A, **(extra or {})}
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app).master(os.environ["SPARK_MASTER"])
    b = b.config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040")).config("spark.ui.showConsoleProgress", "false")
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def mc(*args):
    """Run one mc command against MinIO and return its output."""
    cmd = "mc alias set m %s %s %s >/dev/null && %s" % (ENDPOINT, ACCESS, SECRET, " ".join(args))
    r = subprocess.run(["docker", "run", "--rm", "--network", "host", "--entrypoint", "sh", MC, "-c", cmd],
                       capture_output=True, text=True)
    return r.stdout


class Trace:
    """Record every S3 request that MinIO receives, with `mc admin trace`, while the block runs."""

    NAME = "book-ch38-trace"

    def __enter__(self):
        subprocess.run(["docker", "rm", "-f", self.NAME], capture_output=True)
        cmd = "mc alias set m %s %s %s >/dev/null && mc admin trace --json m" % (ENDPOINT, ACCESS, SECRET)
        subprocess.run(["docker", "run", "-d", "--name", self.NAME, "--network", "host",
                        "--entrypoint", "sh", MC, "-c", cmd], capture_output=True, check=True)
        time.sleep(3)          # let the trace attach before the work starts
        return self

    def __exit__(self, *exc):
        time.sleep(2)          # let the last requests arrive
        out = subprocess.run(["docker", "logs", self.NAME], capture_output=True, text=True).stdout
        subprocess.run(["docker", "rm", "-f", self.NAME], capture_output=True)
        self.calls = []
        for line in out.splitlines():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("type") == "S3" and rec.get("path", "").startswith("/book"):
                self.calls.append(rec)
        return False

    def counts(self):
        """Requests by S3 API name, for example {'PutObject': 64, 'CopyObject': 128}."""
        c = collections.Counter()
        for r in self.calls:
            api = r["api"].removeprefix("s3.")
            c[api] += 1
        return c
