"""Chapter 44, Steps 1 to 3: the worker's sys.path, the interpreter it runs, and what a thin
environment is missing. Each case runs ch44/where.py in a new process.

Run from code/:  uv run python ch44/workers.py
"""
import os
import re
import subprocess
import sys
from pathlib import Path

import pkgs

CODE = Path(__file__).resolve().parents[1]
WHERE = str(CODE / "ch44" / "where.py")
VENV_PY = str(CODE / ".venv" / "bin" / "python")


def short(text):
    text = re.sub(r"/tmp/spark-[0-9a-f-]+/userFiles-[0-9a-f-]+", "<driver tmp>/userFiles-<uuid>", text)
    text = text.replace(str(CODE), "<code>").replace(pkgs.WORK, "<work>")
    return text.replace(os.path.expanduser("~"), "~")


def run(title, cmd=None, cwd=CODE, env=None, conf=""):
    e = dict(os.environ, CH42_CONF=conf, **(env or {}))
    e.pop("PYSPARK_PYTHON", None) if not env or "PYSPARK_PYTHON" not in env else None
    out = subprocess.run(cmd or [VENV_PY, WHERE], cwd=cwd, env=e, capture_output=True, text=True)
    print(f"--- {title}")
    lines = [l for l in out.stdout.splitlines() if l.strip()]
    print(short("\n".join(lines)) or short(out.stderr[-400:]))
    print()


# Step 1: the worker's view, from code/ and from a folder that holds a package.
run("python ch44/where.py, started in code/")
pkg_root = pkgs.package("1.0")                       # <work>/messytools-1.0-*/messytools/
run("the same script, started in a folder that contains messytools/", cwd=pkg_root)

# Step 2: which interpreter the workers run.
run("builder .config('spark.pyspark.python', python3.11)", conf="spark.pyspark.python=python3.11")
run("PYSPARK_PYTHON=python3.11 with a 3.12 driver", env={"PYSPARK_PYTHON": "python3.11"})
submit = str(CODE / ".venv" / "bin" / "spark-submit")
run("spark-submit --conf spark.pyspark.python=/usr/bin/python3.12",
    cmd=[submit, "--master", "local[1]", "--conf", "spark.pyspark.python=/usr/bin/python3.12", WHERE])

# Step 3: a thin worker environment, with tabulate and nothing else.
thin = os.path.join(pkgs.WORK, "thin")
subprocess.run(["/usr/bin/python3.12", "-m", "venv", thin], check=True)
subprocess.run(["uv", "pip", "install", "--quiet", "--python", f"{thin}/bin/python", "tabulate"], check=True)
run("PYSPARK_PYTHON=<thin venv> (tabulate only), default settings",
    env={"PYSPARK_PYTHON": f"{thin}/bin/python"})
run("the same, spark.sql.execution.pythonUDF.arrow.enabled=false",
    env={"PYSPARK_PYTHON": f"{thin}/bin/python"}, conf="spark.sql.execution.pythonUDF.arrow.enabled=false")
subprocess.run(["uv", "pip", "install", "--quiet", "--python", f"{thin}/bin/python", "pyarrow"], check=True)
pv = subprocess.run([f"{thin}/bin/python", "-c", "import pyarrow; print(pyarrow.__version__)"],
                    capture_output=True, text=True).stdout.strip()
has_pandas = subprocess.run([f"{thin}/bin/python", "-c", "import pandas"], capture_output=True).returncode == 0
run(f"the thin venv plus pyarrow {pv} (pandas installed: {has_pandas}), default settings",
    env={"PYSPARK_PYTHON": f"{thin}/bin/python"})
