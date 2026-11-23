"""A write job that fails in its last task: what each committer leaves in the destination.

  uv run python ch87/failure.py            file-v1, file-v2 and magic, one child each
  uv run python ch87/failure.py file-v2    one committer (the child)

Eight tasks on local[4]. Task 7 waits, then fails, so tasks 0 to 6 commit before the job fails.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from s3 import COMMITTERS, mc, s3_session  # noqa: E402


def child(name):
    from pyspark.sql import functions as F
    spark = s3_session(f"ch87-fail-{name}", {**COMMITTERS[name], "spark.task.maxFailures": "1"})

    @F.udf("long")
    def fail_in_task_7(pid, x):
        if pid == 7:
            time.sleep(6)                       # tasks 4, 5 and 6 commit while task 7 waits
            raise ValueError("task 7 fails on purpose")
        return x

    df = spark.range(0, 80000, 1, 8).select(fail_in_task_7(F.spark_partition_id(), "id").alias("id"))
    path = f"s3a://book/fail/{name}"
    try:
        df.write.mode("overwrite").parquet(path)
        print("the write did not fail")
    except Exception as e:
        print("write failed:", str(e).split("\n")[0][:70])
    objs = [json.loads(x) for x in mc("mc ls -r --json", f"m/book/fail/{name}/").splitlines() if x.strip()]
    parts = [o for o in objs if o["key"].split("/")[-1].startswith("part-")]
    temp = [o for o in objs if "_temporary" in o["key"] or "__magic" in o["key"]]
    inc = [x for x in mc("mc ls -r --incomplete", f"m/book/fail/{name}/").splitlines() if x.strip()]
    print(f"RESULT {name}|{len(objs)}|{len(parts)}|{len(temp)}|{len(inc)}|"
          f"{any(o['key'].endswith('_SUCCESS') for o in objs)}")
    print("LIST " + json.dumps([o["key"] for o in objs][:12]))
    # a reader that does not look for _SUCCESS reads the partial output
    if parts:
        n = spark.read.parquet(path).count()
        print(f"READ {n}")


def main():
    print("8 tasks of 10,000 rows. Task 7 fails after tasks 0 to 6 commit.\n")
    print(f"{'committer':10} {'objects':>8} {'part files':>11} {'temp objects':>13} "
          f"{'pending uploads':>16} {'_SUCCESS':>9} {'rows a reader sees':>19}")
    lists = {}
    for n in ("file-v1", "file-v2", "magic"):
        out = subprocess.run([sys.executable, __file__, n], capture_output=True, text=True).stdout
        res = [x for x in out.splitlines() if x.startswith("RESULT ")][0][7:].split("|")
        read = [x for x in out.splitlines() if x.startswith("READ ")]
        lists[n] = json.loads([x for x in out.splitlines() if x.startswith("LIST ")][0][5:])
        rows = read[0][5:] if read else "no data files"
        print(f"{res[0]:10} {res[1]:>8} {res[2]:>11} {res[3]:>13} {res[4]:>16} {res[5]:>9} {rows:>19}")
    print("\nwhat is left under each destination (first keys):")
    for n, keys in lists.items():
        print(f"  {n}:")
        for k in keys[:5]:
            print("    " + (k[:84] if k else "(only the 0-byte directory marker of the path)"))
        if not keys:
            print("    (nothing)")


if __name__ == "__main__":
    child(sys.argv[1]) if len(sys.argv) > 1 else main()
