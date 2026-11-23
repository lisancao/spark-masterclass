"""Chapter 60, Steps 4 to 7: addPyFile and zips, the include list a UDF captures once,
modules that never reload, and native code in a zip against an archive.

Run from code/:  uv run python ch60/pyfiles.py
"""
import os
import re
import sys
import zipfile
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

import pkgs
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ui_port  # noqa: E402


def short(text):
    text = re.sub(r"/tmp/spark-[0-9a-f-]+/userFiles-[0-9a-f-]+", "<userFiles>", str(text))
    return text.replace(pkgs.WORK, "<work>")


def fresh(**conf):
    active = SparkSession.getActiveSession()
    if active:
        active.stop()
    b = SparkSession.builder.master("local[2]").config("spark.ui.port", ui_port())
    for k, v in conf.items():
        b = b.config(k, v)
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("OFF")
    return s


def import_in_tasks(spark, udf):
    """Run udf on two partitions and return the distinct (pid, result) pairs."""
    rows = spark.range(4).repartition(2).select(udf("id").alias("r")).distinct().collect()
    return sorted(short(r.r) for r in rows)


def where_udf():
    @F.udf("string")
    def where(_):
        import os
        try:
            import messytools
            return f"{os.getpid()} {messytools.VERSION} {messytools.__file__}"
        except ImportError as e:
            return f"{os.getpid()} {type(e).__name__}"
    return where


# --- Step 4: addPyFile and the layout of the zip -------------------------------------------------
zpath = pkgs.zip_package("1.0", "messytools.zip")
print("zip entries:", [i.filename for i in zipfile.ZipFile(zpath).infolist()])
spark = fresh()
spark.sparkContext.addPyFile(zpath)
import messytools  # noqa: E402  (works in the driver now: addPyFile edits the driver's sys.path)
print("driver imports:", short(messytools.__file__))
print("tasks import:  ", import_in_tasks(spark, where_udf()))

# --- Step 5: the include list is captured when the UDF first becomes a JVM object ------------------
spark = fresh()
old = where_udf()
print("\nbefore addPyFile:     ", import_in_tasks(spark, old))
print("  captured includes:", list(old._unwrapped._judf.func().pythonIncludes()))
spark.sparkContext.addPyFile(pkgs.zip_package("1.0", "messytools.zip"))
print("old udf, after:       ", import_in_tasks(spark, old))
print("  captured includes:", list(old._unwrapped._judf.func().pythonIncludes()))
new = where_udf()
print("new udf, after:       ", import_in_tasks(spark, new))
print("  captured includes:", list(new._unwrapped._judf.func().pythonIncludes()))
print("old udf, again:       ", import_in_tasks(spark, old))

# --- Step 6: a new version does not reload in a reused worker -------------------------------------
for reuse in ("true", "false"):
    spark = fresh(**{"spark.python.worker.reuse": reuse})
    spark.sparkContext.addPyFile(pkgs.zip_package("1.0", f"messytools-1-{reuse}.zip"))
    v1 = [r.split()[1] for r in import_in_tasks(spark, where_udf())]
    spark.sparkContext.addPyFile(pkgs.zip_package("2.0", f"messytools-2-{reuse}.zip"))
    v2 = [r.split()[1] for r in import_in_tasks(spark, where_udf())]
    print(f"\nspark.python.worker.reuse={reuse}: after v1 {v1}, after v2 {v2}")

# --- Step 7: native code in a zip and in an archive ------------------------------------------------
site = pkgs.site_folder(["ujson", "tabulate"])
for d in sorted(os.listdir(site)):
    if d.endswith(".dist-info"):
        tags = [l.split(": ")[1].strip() for l in open(f"{site}/{d}/WHEEL") if l.startswith("Tag")]
        print(f"\n{d:28} {', '.join(tags)}" if d.startswith("tabulate") else f"{d:28} {', '.join(tags)}")
spark = fresh()
spark.sparkContext.addPyFile(pkgs.zip_folder(site, "deps.zip"))
spark.sparkContext.addArchive(pkgs.tar_folder(os.path.dirname(site), "deps.tar.gz", ".") + "#deps")


@F.udf("string")
def both(_):
    import os, sys
    from pyspark import SparkFiles
    out = []
    for label, path in [("zip", None), ("archive", os.path.join(SparkFiles.get("deps"), "site-packages"))]:
        if path:
            sys.path.insert(0, path)
        for mod in ("tabulate", "ujson"):
            sys.modules.pop(mod, None)
            try:
                m = __import__(mod)
                out.append(f"{label:7} {mod:8} {m.__file__.split('userFiles-')[-1].split('/', 1)[-1]}")
            except ImportError as e:
                out.append(f"{label:7} {mod:8} {type(e).__name__}: {e}")
    return "\n".join(out)


print(spark.range(1).select(both("id")).first()[0])

# A wheel through addPyFile: copied, never put on the path.
whl = os.path.join(pkgs.WORK, "messytools-1.0-py3-none-any.whl")
os.replace(pkgs.zip_package("3.0", "w.zip"), whl)
spark = fresh()
spark.sparkContext.addPyFile(whl)
print("\nmessytools-1.0-py3-none-any.whl via addPyFile:", import_in_tasks(spark, where_udf()))
spark.stop()

# spark.addArtifacts in Classic: a front for addPyFile, and it refuses changed bytes.
spark = fresh()
os.makedirs(f"{pkgs.WORK}/c1", exist_ok=True)
os.makedirs(f"{pkgs.WORK}/c2", exist_ok=True)
first = os.replace(pkgs.zip_package("1.0", "c1.zip"), f"{pkgs.WORK}/c1/messytools.zip") or f"{pkgs.WORK}/c1/messytools.zip"
second = os.replace(pkgs.zip_package("2.0", "c2.zip"), f"{pkgs.WORK}/c2/messytools.zip") or f"{pkgs.WORK}/c2/messytools.zip"
spark.addArtifacts(first, pyfile=True)
print("\nClassic addArtifacts:", import_in_tasks(spark, where_udf()))
spark.addArtifacts(first, pyfile=True)
print("Classic addArtifacts, same bytes again: accepted")
try:
    spark.addArtifacts(second, pyfile=True)
except Exception as e:
    print("Classic addArtifacts, changed bytes:", type(e).__name__, short(str(e).splitlines()[0])[:150])
try:
    spark.sparkContext.addPyFile(second)
except Exception as e:
    cause = next(l for l in str(e).splitlines() if "SparkException" in l)
    print("addPyFile, changed bytes, same file name:", short(cause.split("SparkException: ")[1])[:150])
spark.stop()
