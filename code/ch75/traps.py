"""Chapter 75, Step 4: four ways a correct UDF test fails. Each variant patches a copy of fares.py or
conftest.py, runs pytest on test_fare_band_udf in a new process, and prints the first error line.

Run from code/:  uv run --with pytest python ch75/traps.py   (MODE=connect variants need ch64/server.sh)
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
LAZY = '''def fare_band(col):
    """The UDF, built when it is called: then it belongs to the session of the caller."""
    return F.udf(band, StringType())(col)'''
SHIP = '''    if mode == "connect":
        s.addArtifacts(fares_py, pyfile=True)     # the server's Python workers import fares
    else:
        s.sparkContext.addPyFile(fares_py)        # the executors' Python workers import fares
'''
VARIANTS = [
    ("A. module-level F.udf(band, 'string')", "classic",
     lambda f: f.replace(LAZY, 'fare_band = F.udf(band, "string")'), lambda c: c),
    ("B. module-level UDF, fares.py not shipped to the workers", "classic",
     lambda f: f.replace(LAZY, "fare_band = F.udf(band, StringType())"), lambda c: c.replace(SHIP, "")),
    ("C. module-level UDF, Connect session", "connect",
     lambda f: f.replace(LAZY, "fare_band = F.udf(band, StringType())"), lambda c: c),
    ("D. UDF built at call time, Connect, no addArtifacts", "connect",
     lambda f: f, lambda c: c.replace(SHIP, "")),
    ("E. as in the book: built at call time, shipped", "connect", lambda f: f, lambda c: c),
]

for title, mode, patch_fares, patch_conf in VARIANTS:
    work = Path(tempfile.mkdtemp(prefix="ch75-trap-"))
    (work / "tests").mkdir()
    fares = (HERE / "fares.py").read_text()
    conf = (HERE / "tests" / "conftest.py").read_text()
    assert LAZY in fares and SHIP in conf
    (work / "fares.py").write_text(patch_fares(fares))
    (work / "tests" / "conftest.py").write_text(patch_conf(conf))
    shutil.copy(HERE / "tests" / "test_fares.py", work / "tests")
    env = dict(os.environ, MODE=mode, SPARK_CONNECT_URL=os.environ.get("SPARK_CONNECT_URL", "sc://localhost:15118"))
    r = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-k", "fare_band_udf",
                        str(work / "tests")], capture_output=True, text=True, env=env)
    out = r.stdout + r.stderr
    errs = [line for line in out.splitlines() if re.match(r"^E\s+[\w.]*(Error|Exception):", line)]
    summary = [line for line in out.splitlines() if re.search(r"\d+ (passed|failed|error)", line)]
    print(f"\n== {title} (MODE={mode})")
    print("   " + (summary[-1].strip("= ") if summary else "?"))
    if errs:
        e = re.sub(r"^E\s+([\w]+\.)*", "", errs[-1])
        print("   " + e.replace(str(work), "<tmp>")[:110])
    shutil.rmtree(work)
