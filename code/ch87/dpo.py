"""Dynamic partition overwrite on S3A: the magic committer refuses it, FileOutputCommitter does it.

  uv run python ch87/dpo.py            both, one child process each
"""
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from s3 import COMMITTERS, s3_session  # noqa: E402


def child(name):
    spark = s3_session(f"ch87-dpo-{name}", {**COMMITTERS[name],
                                           "spark.sql.sources.partitionOverwriteMode": "dynamic"})
    path = f"s3a://book/dpo/{name}"
    first = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "day INT, v STRING")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    first.write.mode("overwrite").partitionBy("day").parquet(path)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    again = spark.createDataFrame([(2, "B")], "day INT, v STRING")
    try:
        again.write.mode("overwrite").partitionBy("day").parquet(path)
        rows = sorted(spark.read.parquet(path).collect(), key=lambda r: r.day)
        print(f"  {name:8} overwrite of day=2 worked: " + ", ".join(f"day={r.day} v={r.v}" for r in rows))
    except Exception as e:
        line = [ln for ln in str(e).splitlines() if "dynamicPartitionOverwrite" in ln]
        print(f"  {name:8} " + (line[0].strip() if line else str(e).splitlines()[0])[:150])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        child(sys.argv[1])
    else:
        print("== INSERT OVERWRITE of one partition (partitionOverwriteMode=dynamic)")
        for n in ("magic", "file-v1"):
            print(subprocess.run([sys.executable, __file__, n], capture_output=True, text=True).stdout, end="")
