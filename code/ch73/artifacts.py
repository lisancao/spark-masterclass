"""Chapter 73, Step 10: Spark Connect artifacts. Two sessions on one server ship two versions of
the same zip, each in its own folder and its own Python workers.

Start the Part VIII Connect server first:   ch72/server.sh start
Run from code/:                             uv run python ch73/artifacts.py
"""
import os
import re
import shutil
import sys
from pathlib import Path

from pyspark.sql.connect import functions as F   # the Connect functions: no active session needed
from pyspark.sql.types import StringType
from pyspark.sql.connect.session import SparkSession as ConnectSession

import pkgs

PORT = int(os.environ.get("CONNECT_PORT", "10280"))
URL = f"sc://localhost:{PORT}"


def short(text):
    """Keep the first 8 characters of each UUID, and drop the server's temporary folders."""
    text = re.sub(r"/tmp/spark-[0-9a-f-]+/userFiles-[0-9a-f-]+", "<root>", str(text))
    text = re.sub(r"([0-9a-f]{8})-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", r"\1", text)
    return text.replace(pkgs.WORK, "<work>")


def where(spark):
    """Import messytools (from the zip) and messyarch (from the archive) inside a task."""
    @F.udf(StringType())   # a DDL string needs an active session, and create() sets none
    def probe(_):
        import os, sys
        from pyspark import SparkFiles
        out = [f"pid={os.getpid()}", f"cwd={os.getcwd()}", f"root={SparkFiles.getRootDirectory()}"]
        try:
            import messytools
            out.append(f"zip={messytools.VERSION}")
        except ImportError:
            out.append("zip=missing")
        try:
            site = os.path.join(SparkFiles.get("deps"), "site-packages")
            if os.path.isdir(site) and site not in sys.path:
                sys.path.insert(0, site)
            import messyarch
            out.append(f"archive={messyarch.__file__}")
        except ImportError:
            out.append("archive=missing")
        return " ".join(out)
    return spark.range(1).select(probe("id")).first()[0]


def site_archive(version):
    """deps.tar.gz with site-packages/messyarch/__init__.py at the top."""
    root = pkgs.package(version, name="messyarch")
    site = os.path.join(pkgs.WORK, f"arch-{version}", "site-packages")
    shutil.copytree(f"{root}/messyarch", f"{site}/messyarch")
    return pkgs.tar_folder(os.path.dirname(site), f"deps-{version}.tar.gz", ".")


a = ConnectSession.builder.remote(URL).create()
b = ConnectSession.builder.remote(URL).create()
try:
    print("client session_id   A:", short(a.client._session_id), " B:", short(b.client._session_id))

    # Session A ships version 2.0 of messytools and an archive; session B ships version 3.0.
    os.makedirs(f"{pkgs.WORK}/a", exist_ok=True)
    os.makedirs(f"{pkgs.WORK}/b", exist_ok=True)
    zip_a = shutil.move(pkgs.zip_package("2.0", "a.zip"), f"{pkgs.WORK}/a/messytools.zip")
    zip_b = shutil.move(pkgs.zip_package("3.0", "b.zip"), f"{pkgs.WORK}/b/messytools.zip")
    print("before addArtifacts A:", short(where(a)))
    a.addArtifacts(zip_a, pyfile=True)
    a.addArtifacts(site_archive("1.1") + "#deps", archive=True)
    b.addArtifacts(zip_b, pyfile=True)
    print("after                A:", short(where(a)))
    print("after                B:", short(where(b)))

    # The same name again: identical bytes are accepted, different bytes are refused.
    a.addArtifacts(zip_a, pyfile=True)
    print("A adds the same bytes again: accepted")
    os.makedirs(f"{pkgs.WORK}/a2", exist_ok=True)
    changed = shutil.move(pkgs.zip_package("2.1", "a2.zip"), f"{pkgs.WORK}/a2/messytools.zip")
    try:
        a.addArtifacts(changed, pyfile=True)
    except Exception as e:
        print("A adds changed bytes:", type(e).__name__, short(str(e).splitlines()[0])[:160])

    # What the client refuses before it sends anything.
    whl = os.path.join(pkgs.WORK, "messytools-1.0-py3-none-any.whl")
    shutil.copy(zip_a, whl)
    for label, path in [("s3 path", "s3://bucket/messytools.zip"), ("wheel", whl)]:
        try:
            a.addArtifacts(path, pyfile=True)
        except Exception as e:
            print(f"{label:8} ->", type(e).__name__, short(str(e).splitlines()[0])[:120])

finally:
    for s in (b, a):           # release both sessions on the server, then close the channels
        s.client.release_session()
        s.client.close()
