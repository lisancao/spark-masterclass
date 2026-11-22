"""Chapter 74: set one key in many places, and read the value that wins.

Each row starts a new Classic session in a child process with one more configuration
source than the row above it. The child prints three values:
  spark.sql.shuffle.partitions  (a runtime SQL config)
  spark.driver.memory           (a core config, read from the SparkConf)
  the driver JVM's maximum heap (what spark.driver.memory really did at launch)
Then one session changes the SQL config at runtime with spark.conf.set, SET and RESET.

Run from code/:  uv run python ch74/layers.py > ch74/output/layers.txt
"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
KEY = "spark.sql.shuffle.partitions"

CHILD = r'''
import json, os, sys
sys.path.insert(0, sys.argv[1])
from pyspark.sql import SparkSession
b = SparkSession.builder.master("local[2]").config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040"))
for k, v in json.loads(os.environ.get("BUILDER_CONF", "{}")).items():
    b = b.config(k, v)
spark = b.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
heap = spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory() // (1024 * 1024)
print(json.dumps({
    "shuffle": spark.conf.get("spark.sql.shuffle.partitions"),
    "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory", "(not set)"),
    "heap_mib": heap,
}))
spark.stop()
'''


def run(label, conf_dir=None, submit_args=None, builder=None):
    env = dict(os.environ)
    env.pop("SPARK_CONF_DIR", None)
    env.pop("PYSPARK_SUBMIT_ARGS", None)
    if conf_dir:
        env["SPARK_CONF_DIR"] = str(conf_dir)
    if submit_args:
        env["PYSPARK_SUBMIT_ARGS"] = submit_args
    if builder:
        env["BUILDER_CONF"] = json.dumps(builder)
    out = subprocess.run([sys.executable, "-c", CHILD, str(HERE.parent)], env=env,
                         capture_output=True, text=True, check=True).stdout
    r = json.loads(out.strip().splitlines()[-1])
    print(f"  {label:<44} {r['shuffle']:>8}  {r['driver_memory']:>13}  {r['heap_mib']:>6} MiB")


with tempfile.TemporaryDirectory() as tmp:
    conf = Path(tmp) / "conf"
    conf.mkdir()
    print("== 1. one key, more sources each time (a new session for each row)")
    print(f"  {'sources':<44} {'shuffle':>8}  {'driver.memory':>13}  {'JVM heap':>10}")
    run("built-in defaults only")

    (conf / "spark-env.sh").write_text("export SPARK_DRIVER_MEMORY=3g\n")
    run("+ spark-env.sh (SPARK_DRIVER_MEMORY=3g)", conf)

    (conf / "spark-defaults.conf").write_text(
        f"{KEY}  11\nspark.driver.memory  1g\n")
    run("+ spark-defaults.conf (11, 1g)", conf)

    args = f"--conf {KEY}=12 --conf spark.driver.memory=2g pyspark-shell"
    run("+ --conf (12, 2g)", conf, args)

    args = f"--conf {KEY}=12 --conf spark.driver.memory=2g --driver-memory 1500m pyspark-shell"
    run("+ --driver-memory 1500m", conf, args)

    run("+ builder.config (13, 4g)", conf, args, {KEY: "13", "spark.driver.memory": "4g"})

    print("  -- the same builder.config, with fewer sources under it")
    args = f"--conf {KEY}=12 --conf spark.driver.memory=2g pyspark-shell"
    run("--conf (12, 2g) + builder.config", conf, args, {KEY: "13", "spark.driver.memory": "4g"})
    run("builder.config (13, 4g) only", None, None, {KEY: "13", "spark.driver.memory": "4g"})

# The runtime layer: one session, the SQL config changed after it started.
sys.path.insert(0, str(HERE.parent))
from common.book import session   # noqa: E402

spark = session("ch74-layers", **{"spark.sql.shuffle.partitions": "13"})
print("\n== 2. runtime changes in one session (the builder set 13)")
show = lambda label: print(f"  {label:<44} {spark.conf.get(KEY)}")
show("after getOrCreate")
spark.conf.set(KEY, "14")
show("after spark.conf.set(..., 14)")
spark.sql(f"SET {KEY}=15")
show(f"after SET {KEY}=15")
spark.sql(f"SET {KEY}").show(truncate=False)
spark.sql(f"RESET {KEY}")
show(f"after RESET {KEY}")
spark.conf.set(KEY, "16")
spark.sql("RESET")
show("after RESET (all keys)")
spark.conf.unset(KEY)
show("after spark.conf.unset")

print("== 3. a second session on the same SparkContext")
other = spark.newSession()
spark.conf.set(KEY, "17")
print(f"  first session : {spark.conf.get(KEY)}")
print(f"  newSession()  : {other.conf.get(KEY)}")
spark.stop()
