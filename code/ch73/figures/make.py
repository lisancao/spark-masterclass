"""Chapter 73 figures. Paths and results from code/ch73/output/workers.txt and pyfiles.txt."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def syspath():
    f = Fig(900, 330, "The first entries of sys.path in a Python worker, in local mode and on a cluster")
    f.text(20, 26, "sys.path of a Python worker, from the top", size=14, weight=700)
    f.text(330, 52, "local mode (where.py output)", size=13, weight=600, c="go")
    f.text(640, 52, "on a cluster", size=13, weight=600, c="si")
    rows = [("1. current folder", "the folder where the driver started", "a work folder of the executor", "bad"),
            ("2. files root", "<driver tmp>/userFiles-<uuid>", "the current folder of the executor", "gr"),
            ("3. pyspark.zip", "from the Spark install", "from the Spark install", "gr"),
            ("4. py4j zip", "py4j-0.10.9.9-src.zip", "py4j-0.10.9.9-src.zip", "gr"),
            ("then site-packages", "of pythonExec (PYSPARK_PYTHON)", "of pythonExec (PYSPARK_PYTHON)", "gr")]
    for i, (name, loc, clu, c) in enumerate(rows):
        y = 64 + i * 44
        f.rect(20, y, 860, 38, c=c, fill=0.12 if c == "bad" else 0.06, rx=5, stroke=1 if c == "bad" else 0.8)
        f.text(32, y + 24, name, size=13, weight=600)
        f.text(330, y + 24, loc, size=12, mono=("<" in loc or ".zip" in loc))
        f.text(640, y + 24, clu, size=12, mono=(".zip" in clu))
    f.text(20, 300, "Entry 1 is why a package in the driver folder imports in local mode and fails on a cluster.",
           size=12, c="bad")
    f.text(20, 318, "setup_spark_files adds the files root and each shipped include for each task.", size=12, c="mute")
    return f.save("ch73-syspath", CHECK)


def zip_archive():
    f = Fig(900, 300, "The same site folder shipped as a zip and as an archive")
    f.text(20, 26, "One site folder (tabulate: pure Python, ujson: native .so), shipped two ways", size=14, weight=700)
    for x, title, how, res in ((20, "deps.zip with addPyFile", "zipimport: the zip is a folder on sys.path",
                                [("tabulate", "deps.zip/tabulate/__init__.py", "ok"),
                                 ("ujson", "ModuleNotFoundError", "bad")]),
                               (460, "deps.tar.gz with addArchive", "unpacked into a real folder deps/",
                                [("tabulate", "deps/site-packages/tabulate/__init__.py", "ok"),
                                 ("ujson", "deps/site-packages/ujson.cpython-312-x86_64-linux-gnu.so", "ok")])):
        f.rect(x, 44, 420, 200, c="gr", fill=0.05, rx=8)
        f.text(x + 14, 70, title, size=14, weight=700, mono=False)
        f.text(x + 14, 90, how, size=12, c="mute")
        for j, (m, path, c) in enumerate(res):
            y = 112 + j * 60
            f.rect(x + 14, y, 392, 48, c=c, fill=0.15, rx=5)
            f.text(x + 26, y + 20, f"import {m}", size=13, mono=True, weight=600)
            f.text(x + 26, y + 38, path, size=10.5, mono=True, c="ok" if c == "ok" else "bad")
    f.text(20, 268, "A zip can load .py files only. Native code needs a real folder. Build it for the executors:",
           size=12)
    f.text(20, 286, "ujson-6.1.0 is cp312, manylinux2014_x86_64: CPython 3.12 on x86-64 Linux only.", size=12, c="mute")
    return f.save("ch73-zip-archive", CHECK)


if __name__ == "__main__":
    for fn in (syspath, zip_archive):
        print(fn())
