"""Chapter 73, Steps 8 and 9: spark.submit.pyFiles in the builder, a packed venv in local mode,
a portable site-folder archive, and the packaging guide's `uv run` recipe.

Run from code/:  uv run python ch73/archives.py
"""
import os
import re
import subprocess
import sys
import tarfile
import textwrap
from pathlib import Path

import pkgs

CODE = Path(__file__).resolve().parents[1]
VENV_PY = str(CODE / ".venv" / "bin" / "python")


def short(text):
    text = re.sub(r"/tmp/spark-[0-9a-f-]+/userFiles-[0-9a-f-]+", "<userFiles>", text)
    text = re.sub(r"environments-v2/[\w-]+", "environments-v2/<hash>", text)
    return text.replace(pkgs.WORK, "<work>").replace(os.path.expanduser("~"), "~")


def run_script(name, body, env=None, cwd=None):
    path = os.path.join(pkgs.WORK, name)
    with open(path, "w") as f:
        f.write(textwrap.dedent(body))
    e = dict(os.environ, **(env or {}))
    return subprocess.run(env.get("CMD", VENV_PY).split() + [path] if env else [VENV_PY, path],
                          cwd=cwd or pkgs.WORK, env=e, capture_output=True, text=True)


SESSION = f"""
    from pyspark.sql import SparkSession, functions as F
    spark = SparkSession.builder.master("local[1]").config("spark.ui.port", "{os.environ.get('SPARK_UI_PORT', '4040')}")
"""

# --- spark.submit.pyFiles set in the builder of a plain `python app.py` ----------------------------
zpath = pkgs.zip_package("1.0", "messytools.zip")
out = run_script("pyfiles_conf.py", SESSION + f"""
    spark = spark.config("spark.submit.pyFiles", "{zpath}").getOrCreate()
    import messytools
    f = F.udf(lambda _: __import__("messytools").__file__, "string")
    print("driver:", messytools.__file__)
    print("task:  ", spark.range(1).select(f("id")).first()[0])
""")
print("spark.submit.pyFiles in the builder:\n" + short(out.stdout))

# --- A packed venv, the cluster recipe, run in local mode ------------------------------------------
venv = os.path.join(pkgs.WORK, "pyspark_venv")
subprocess.run(["/usr/bin/python3.12", "-m", "venv", venv], check=True)
subprocess.run(["uv", "pip", "install", "--quiet", "--python", f"{venv}/bin/python",
                "venv-pack", "pyarrow", "tabulate"], check=True)
packed = os.path.join(pkgs.WORK, "pyspark_venv.tar.gz")
subprocess.run([f"{venv}/bin/venv-pack", "-q", "-p", venv, "-o", packed], check=True)
links = [f"{m.name} -> {m.linkname}" for m in tarfile.open(packed) if m.name.startswith("bin/python")]
print(f"venv-pack: {os.path.getsize(packed) / 1e6:.0f} MB;", "; ".join(sorted(links)))

out = run_script("packed_env.py", SESSION + f"""
    spark = spark.config("spark.archives", "{packed}#environment").getOrCreate()
    import os
    from pyspark import SparkFiles
    print("files root:", SparkFiles.getRootDirectory(), os.listdir(SparkFiles.getRootDirectory()))
    f = F.udf(lambda _: __import__("sys").executable, "string")
    try:
        print(spark.range(1).select(f("id")).first()[0])
    except Exception as e:
        print(next(l for l in str(e).splitlines() if "Cannot run program" in l).strip().lstrip(": "))
""", env={"PYSPARK_PYTHON": "./environment/bin/python"})
print("PYSPARK_PYTHON=./environment/bin/python, spark.archives=pyspark_venv.tar.gz#environment:")
print(short(out.stdout))

# --- The portable alternative: a site folder in an archive, put on sys.path inside the task --------
site = pkgs.site_folder(["tabulate"])
arch = pkgs.tar_folder(os.path.dirname(site), "deps.tar.gz", ".")
out = run_script("site_folder.py", SESSION + f"""
    spark = spark.getOrCreate()
    spark.sparkContext.addArchive("{arch}#deps")

    @F.udf("string")
    def uses_deps(_):
        import os, sys
        from pyspark import SparkFiles
        site = os.path.join(SparkFiles.get("deps"), "site-packages")
        if site not in sys.path:
            sys.path.insert(0, site)
        import tabulate
        return tabulate.__file__

    print("task imports:", spark.range(1).select(uses_deps("id")).first()[0])
""")
print("a site folder shipped with addArchive:\n" + short(out.stdout))

# --- The packaging guide's uv run recipe ----------------------------------------------------------
wrapper = os.path.join(pkgs.WORK, "uv_run")
with open(wrapper, "w") as f:
    f.write('#!/bin/sh\nexec uv run "$@"\n')
os.chmod(wrapper, 0o755)
spark_home = subprocess.run([VENV_PY, "-c", "import pyspark,os;print(os.path.dirname(pyspark.__file__))"],
                            capture_output=True, text=True).stdout.strip()
app = os.path.join(pkgs.WORK, "uv_app.py")
with open(app, "w") as f:
    f.write(textwrap.dedent(f"""\
        # /// script
        # dependencies = ["tabulate==0.9.0", "pyspark[connect]==4.3.0"]
        # ///
        import sys
        from pyspark.sql import SparkSession, functions as F
        import tabulate
        import os
        b = SparkSession.builder.master("local[1]").config("spark.ui.port", "{os.environ.get('SPARK_UI_PORT', '4040')}")
        if os.environ.get("CH42_EXECUTOR_VENV") is not None:      # an executor that does not inherit the driver's env
            b = (b.config("spark.executorEnv.VIRTUAL_ENV", os.environ["CH42_EXECUTOR_VENV"])
                  .config("spark.sql.execution.pythonUDF.arrow.enabled", "false"))
        spark = b.getOrCreate()
        spark.sparkContext.setLogLevel("OFF")

        @F.udf("string")
        def worker(_):
            import sys
            try:
                import tabulate
                return f"{{sys.executable}} tabulate {{tabulate.__version__}}"
            except ImportError as e:
                return f"{{sys.executable}} {{e!r}}"

        print("driver:", sys.executable, "tabulate", tabulate.__version__)
        print("VIRTUAL_ENV in the driver:", os.environ.get("VIRTUAL_ENV"))
        print("worker:", spark.range(1).select(worker("id")).first()[0])
    """))
env = {k: v for k, v in os.environ.items() if k not in ("VIRTUAL_ENV",)}
env.update(PYSPARK_PYTHON="./uv_run", SPARK_HOME=spark_home)
uv_version = subprocess.run(["uv", "--version"], capture_output=True, text=True).stdout.split()[1]
for title, extra in [("local mode, executors inherit the driver's environment", {}),
                     ("spark.executorEnv.VIRTUAL_ENV='' (an executor on another host)", {"CH42_EXECUTOR_VENV": ""})]:
    out = subprocess.run(["uv", "run", "--script", app], cwd=pkgs.WORK, env=dict(env, **extra),
                         capture_output=True, text=True)
    print(f"PYSPARK_PYTHON=./uv_run, uv run --script app.py (uv {uv_version}), {title}:")
    print(short(out.stdout) or short(out.stderr[-600:]))
