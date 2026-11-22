"""Shared code for Chapter 88: the PostgreSQL connection, a session with the driver, and the server log.

PostgreSQL runs in Docker (ch88/pg.sh up) with log_statement=all, so `Log` can show the SQL
that Spark sent. The password is a test password for that container only.
"""
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

URL = "jdbc:postgresql://localhost:15515/book"
PROPS = {"user": "book", "password": "book-secret", "driver": "org.postgresql.Driver"}
DRIVER = "org.postgresql:postgresql:42.7.11"     # the PostgreSQL JDBC driver, from Maven Central
CONTAINER = "book-ch88-pg"
CATALOG = {   # a JDBC catalog named "pg" over the same database
    "spark.sql.catalog.pg": "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
    "spark.sql.catalog.pg.url": URL,
    "spark.sql.catalog.pg.user": PROPS["user"],
    "spark.sql.catalog.pg.password": PROPS["password"],
    "spark.sql.catalog.pg.driver": PROPS["driver"],
}


def pg_session(app, extra=None, master="local[4]"):
    """A classic local session with the PostgreSQL driver and the "pg" catalog."""
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName(app).master(os.environ.get("SPARK_MASTER", master))
         .config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040"))
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.jars.packages", DRIVER))
    for k, v in {**CATALOG, **(extra or {})}.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def psql(sql):
    """Run SQL in the container with psql and return its text output."""
    r = subprocess.run(["docker", "exec", "-i", CONTAINER, "psql", "-U", "book", "-d", "book",
                        "-v", "ON_ERROR_STOP=1", "-X", "-q", "-c", sql],
                       capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(r.stderr)
    return r.stdout


class Log:
    """The statements that PostgreSQL logged while the block ran (from `docker logs`)."""

    def __enter__(self):
        self.tag = "mark_" + uuid.uuid4().hex[:8]
        psql(f"SELECT 1 AS {self.tag}")
        return self

    def __exit__(self, *exc):
        psql(f"SELECT 2 AS {self.tag}")
        out = subprocess.run(["docker", "logs", CONTAINER], capture_output=True, text=True)
        text = out.stdout + out.stderr
        lines = text.splitlines()
        idx = [i for i, ln in enumerate(lines) if self.tag in ln]
        body = lines[idx[0] + 1: idx[-1]] if len(idx) >= 2 else []
        self.lines = []
        for ln in body:
            m = re.search(r"LOG:  (?:execute [^:]+|statement): (.*)$", ln)
            if m:
                self.lines.append(m.group(1))
            elif self.lines and not re.match(r"^\d{4}-\d\d-\d\d ", ln):
                self.lines[-1] += " " + ln.strip()          # a statement that spans lines
        return False

    def selects(self, table=None):
        """The SELECT statements, without the metadata queries that the driver sends."""
        out = [s for s in self.lines if s.upper().startswith("SELECT") and "pg_catalog" not in s
               and "information_schema" not in s.lower()]
        if table:
            out = [s for s in out if table in s]
        return out
