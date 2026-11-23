"""Write the same taxi month to MinIO with each output committer, and count what each one costs.

  uv run python ch87/committers.py            all committers, one child process each
  uv run python ch87/committers.py magic      one committer (the child)

Each child starts its own session (SparkContext sets the committer defaults at start-up),
writes one warm-up copy and three timed copies, and traces the S3 requests of the last copy.
"""
import json
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from s3 import COMMITTERS, Trace, mc, s3_session  # noqa: E402
from common.book import TRIPS, ui_api  # noqa: E402

FILES = 32       # output files for each write
RUNS = 3         # timed writes after one warm-up


def metric(name, execution):
    for n in execution["nodes"]:
        for m in n["metrics"]:
            if m["name"] == name:
                return m["value"]
    return None


def seconds(v):
    """'1.8 s (76 ms, ...)' or '227 ms' -> seconds of the total."""
    first = v.split("\n")[-1] if "total" in v else v
    num, unit = re.match(r"([\d.,]+) (ms|s|m)", first).groups()
    x = float(num.replace(",", ""))
    return {"ms": x / 1000, "s": x, "m": x * 60}[unit]


def child(name):
    spark = s3_session(f"ch87-{name}", COMMITTERS[name])
    df = spark.read.parquet(str(TRIPS)).repartition(FILES).cache()
    df.count()
    base = f"s3a://book/commit/{name}"
    rows = []
    for i in range(RUNS + 1):
        path = f"{base}/run{i}"
        if i == RUNS:
            tr = Trace().__enter__()
        t = time.perf_counter()
        df.write.mode("overwrite").parquet(path)
        wall = time.perf_counter() - t
        if i == RUNS:
            tr.__exit__(None, None, None)
        ex = [e for e in ui_api("sql?details=true") if metric("job commit time", e)][-1]
        rows.append((wall, seconds(metric("task commit time", ex)), seconds(metric("job commit time", ex))))
    timed = rows[1:]
    hc = spark._jsc.hadoopConfiguration()
    out = {
        "committer": name,
        "commit protocol": spark.conf.get("spark.sql.sources.commitProtocolClass").split(".")[-1],
        "wall s": round(statistics.median(r[0] for r in timed), 2),
        "task commit s (sum)": round(statistics.median(r[1] for r in timed), 2),
        "job commit s": round(statistics.median(r[2] for r in timed), 2),
        "requests": dict(sorted(tr.counts().items(), key=lambda kv: -kv[1])),
        "algorithm.version": hc.get("mapreduce.fileoutputcommitter.algorithm.version"),
    }
    listing = mc("mc ls -r --json", f"m/book/commit/{name}/run{RUNS}/")
    objs = [json.loads(x) for x in listing.splitlines() if x.strip()]
    out["objects"] = len(objs)
    out["part files"] = sum(1 for o in objs if o["key"].split("/")[-1].startswith("part-"))
    succ = [o for o in objs if o["key"].endswith("_SUCCESS")]
    out["_SUCCESS bytes"] = succ[0]["size"] if succ else None
    if succ and succ[0]["size"] > 0:
        body = json.loads(mc("mc cat", f"m/book/commit/{name}/run{RUNS}/_SUCCESS"))
        out["_SUCCESS name"] = body.get("name")
        out["_SUCCESS committer"] = body.get("committer")
        out["_SUCCESS files listed"] = len(body.get("filenames", []))
        keep = ("object_copy_requests", "object_put_request", "object_multipart_initiated",
                "committer_commits_completed", "op_rename", "object_list_request",
                "committer_bytes_committed", "object_delete_request")
        out["_SUCCESS metrics"] = {k: v for k, v in sorted(body.get("metrics", {}).items()) if k in keep}
        if name == "magic":
            body["hostname"] = "<host>"
            Path(HERE / "output" / "success-magic.json").write_text(json.dumps(body, indent=1)[:4000] + "\n...\n")
    print("RESULT " + json.dumps(out))


def main():
    names = list(COMMITTERS)
    results = []
    for n in names:
        r = subprocess.run([sys.executable, __file__, n], capture_output=True, text=True)
        line = [x for x in r.stdout.splitlines() if x.startswith("RESULT ")]
        if not line:
            print(r.stderr[-3000:])
            raise SystemExit(f"{n} failed")
        results.append(json.loads(line[0][7:]))
    print(f"{FILES} files of the taxi month (3,837,248 rows), MinIO in Docker, local[4]")
    print(f"median of {RUNS} writes after a warm-up; requests from the last write\n")
    print(f"{'committer':12} {'wall s':>7} {'task commit':>12} {'job commit':>11} "
          f"{'PUT':>5} {'COPY':>5} {'DELETE':>7} {'LIST':>5} {'HEAD':>5} {'MPU':>5} {'_SUCCESS':>9}")
    for r in results:
        q = r["requests"]
        mpu = q.get("NewMultipartUpload", 0)
        dels = q.get("DeleteObject", 0) + q.get("DeleteMultipleObjects", 0)
        print(f"{r['committer']:12} {r['wall s']:7.2f} {r['task commit s (sum)']:11.2f}s {r['job commit s']:10.2f}s "
              f"{q.get('PutObject', 0):5} {q.get('CopyObject', 0):5} {dels:7} {q.get('ListObjectsV2', 0):5} "
              f"{q.get('HeadObject', 0):5} {mpu:5} {str(r['_SUCCESS bytes']) + ' B':>9}")
    print("\nall requests of the last write, by S3 API:")
    for r in results:
        print(f"  {r['committer']:12} " + ", ".join(f"{k} {v}" for k, v in r["requests"].items()))
    print("\nwhat _SUCCESS says:")
    for r in results:
        if r.get("_SUCCESS name"):
            print(f"  {r['committer']:12} {r['_SUCCESS name']}, committer={r['_SUCCESS committer']}, "
                  f"{r['_SUCCESS files listed']} files listed")
        else:
            print(f"  {r['committer']:12} empty marker ({r['_SUCCESS bytes']} B)")
    Path(HERE / "output" / "committers.json").write_text(json.dumps(results, indent=1) + "\n")


if __name__ == "__main__":
    child(sys.argv[1]) if len(sys.argv) > 1 else main()
