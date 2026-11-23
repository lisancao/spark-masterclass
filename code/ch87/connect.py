"""Connect Spark to MinIO with S3A: where the keys come from, a write, a listing and a read back.

  uv run python ch87/connect.py           the parent: runs the three children below
  uv run python ch87/connect.py conf      keys in the Spark configuration (spark.hadoop.fs.s3a.*)
  uv run python ch87/connect.py env       keys in AWS_* environment variables, none in the configuration
  uv run python ch87/connect.py wrong     a wrong secret key
"""
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from s3 import ACCESS, CLOUD, ENDPOINT, S3A, SECRET, mc, s3_session  # noqa: E402
from common.book import TRIPS  # noqa: E402


def sources(spark, keys):
    hc = spark._jsc.hadoopConfiguration()
    for k in keys:
        v = hc.get(k)
        src = hc.getPropertySources(k)
        shown = "****" if ("secret" in k and v) else v
        print(f"  {k} = {shown}")
        print(f"      source: {list(src)[-1] if src else None}")


def conf():
    spark = s3_session("ch87-conf")
    print("== keys in the Spark configuration")
    sources(spark, ["fs.s3a.endpoint", "fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.path.style.access"])
    print("\n== the default credential provider chain (core-default.xml)")
    for p in spark._jsc.hadoopConfiguration().get("fs.s3a.aws.credentials.provider").split(","):
        if p.strip():
            print("  " + p.strip())
    print("\n== write the taxi month to s3a://book/trips/")
    df = spark.read.parquet(str(TRIPS))
    t = time.perf_counter()
    df.repartition(8).write.mode("overwrite").parquet("s3a://book/trips/")
    print(f"  wrote {df.count():,} rows in {time.perf_counter() - t:.1f} s (not a benchmark)")
    print("\n== what MinIO holds (mc ls)")
    for line in mc("mc ls -r", "m/book/trips/").splitlines()[:4]:
        print("  " + line.split("STANDARD ")[-1].strip() + "   " + line.split("]")[1].split("STANDARD")[0].strip())
    print("  ...")
    print("\n== read it back through S3A, with a filter")
    back = spark.read.parquet("s3a://book/trips/").where("trip_distance > 20")
    print(f"  trips longer than 20 miles: {back.count():,}")
    fs = spark._jvm.org.apache.hadoop.fs.Path("s3a://book/").getFileSystem(spark._jsc.hadoopConfiguration())
    print(f"  file system class: {fs.getClass().getName()}")


def env():
    # The session gets no fs.s3a keys. SparkContext copies the AWS_* variables into them.
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("ch87-env")
             .config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040"))
             .config("spark.jars.packages", CLOUD)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    print("== keys from AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
    sources(spark, ["fs.s3a.endpoint", "fs.s3a.access.key", "fs.s3a.secret.key"])
    print(f"  rows read: {spark.read.parquet('s3a://book/trips/').count():,}")


def wrong():
    spark = s3_session("ch87-wrong", {"spark.hadoop.fs.s3a.secret.key": "not-the-secret"})
    print("== a wrong secret key")
    try:
        spark.read.parquet("s3a://book/trips/").count()
    except Exception as e:
        msg = str(e)
        line = [ln for ln in msg.splitlines() if "AccessDenied" in ln or "403" in ln or "Forbidden" in ln]
        print("  " + (line[0] if line else msg.splitlines()[0])[:220])


def main():
    for mode in ("conf", "env", "wrong"):
        e = dict(os.environ)
        if mode == "env":
            e.update(AWS_ENDPOINT_URL=ENDPOINT, AWS_ACCESS_KEY_ID=ACCESS, AWS_SECRET_ACCESS_KEY=SECRET)
        r = subprocess.run([sys.executable, __file__, mode], capture_output=True, text=True, env=e)
        print(r.stdout)
        if r.returncode:
            print(r.stderr[-2000:])


if __name__ == "__main__":
    {"conf": conf, "env": env, "wrong": wrong}[sys.argv[1]]() if len(sys.argv) > 1 else main()
