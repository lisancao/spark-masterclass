"""The INFO log of the magic committer for a write of one file: the __magic path and the .pendingset.

  uv run python ch87/magic_log.py     runs the write in a child process and keeps the committer lines
"""
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))


def child():
    from s3 import s3_session
    spark = s3_session("ch87-magic-log")
    jvm = spark._jvm
    level = jvm.org.apache.logging.log4j.Level.INFO
    for logger in ("org.apache.hadoop.fs.s3a.commit", "org.apache.hadoop.mapreduce.lib.output"):
        jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(logger, level)
    spark.range(10).coalesce(1).write.mode("overwrite").parquet("s3a://book/magic-log")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        child()
        raise SystemExit
    err = subprocess.run([sys.executable, __file__, "child"], capture_output=True, text=True).stderr
    keep = ("MagicCommitTracker", "Saving work", "committed 1 files", "Using committer magic",
            "Committing files", "Starting: Task committer", "Writing success file")
    for line in err.splitlines():
        if any(k in line for k in keep):
            line = re.sub(r"^\S+ \S+ ", "", line)                        # drop the date and time
            line = re.sub(r"attempt_\d+_", "attempt_..._", line)
            line = re.sub(r"task_\d+_", "task_..._", line)
            line = re.sub(r"job-([0-9a-f]{4})[0-9a-f-]{32}", r"job-\1...", line)
            line = re.sub(r"__magic_job-\w{4}\.\.\./job-\w{4}\.\.\./", "__magic_job-.../", line)
            line = line.replace("s3a://book/magic-log/", "").replace("-c000.snappy.parquet", "...parquet")
            line = re.sub(r"part-00000-[0-9a-f-]{36}", "part-00000-...", line)
            print(line[:160])
