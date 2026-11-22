"""Chapter 1: one iterative job, written the MapReduce way and the RDD way, measured.

The job fits fare = a + b * distance to the June taxi trips with batch gradient descent.
Each pass reads every trip once. That is the shape of the jobs that the RDD paper used.

  mapreduce  each pass is its own job: read the text input from disk, parse it, map each
             point to a partial gradient, shuffle and reduce, write the new weights to disk.
             The next pass reads those weights back from the file, the way chained
             MapReduce jobs pass state through HDFS.
  rdd        parse the input once, cache() the points in executor memory, and run each pass
             against the cached partitions. The weights travel to the tasks in the closure.

Classic only (RDDs are not in Spark Connect).
Run from code/:  uv run python ch01/iterative.py [passes]
"""
import json
import shutil
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, ui_api, work, tree, ROOT, TRIPS

PASSES = int(sys.argv[1]) if len(sys.argv) > 1 else 10
RATE = 0.01                       # learning rate (distance is in miles, so keep it small)

spark = session("ch01-iterative", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext
w = work("ch01")
points_dir = w / "points"

# The input, as a MapReduce job sees it: lines of text, one point per line.
(spark.read.parquet(str(TRIPS))
 .where("trip_distance > 0 AND trip_distance < 50 AND fare_amount > 0 AND fare_amount < 250")
 .select(F.format_string("%.2f,%.2f", "trip_distance", "fare_amount").alias("value"))
 .repartition(16)
 .write.text(str(points_dir)))
in_bytes = sum(p.stat().st_size for p in points_dir.glob("part-*"))
n_files = len(list(points_dir.glob("part-*")))


def parse(line):
    d, f = line.split(",")
    return float(d), float(f)


def gradient(p, a, b):
    """The partial gradient of the squared error for one point."""
    x, y = p
    err = (a + b * x) - y
    return (err, err * x, 1)


def add(u, v):
    return (u[0] + v[0], u[1] + v[1], u[2] + v[2])


def step(a, b, g):
    return a - RATE * g[0] / g[2], b - RATE * g[1] / g[2]


def stage_io(desc):
    """Bytes read from files, bytes read from cache and bytes written, for the jobs with this description."""
    jobs = [j for j in ui_api("jobs") if j.get("description") == desc]
    read = shuf = written = 0
    for j in jobs:
        for sid in j["stageIds"]:
            for st in ui_api(f"stages/{sid}"):
                if st["status"] == "COMPLETE":
                    read += st["inputBytes"]
                    shuf += st["shuffleWriteBytes"]
                    written += st["outputBytes"]
    return read, shuf, written


def mapreduce(passes):
    a = b = 0.0
    wdir = w / "weights"
    state = wdir / "pass-0"
    sc.parallelize([json.dumps([a, b])], 1).saveAsTextFile(str(state))
    times, io = [], []
    for i in range(1, passes + 1):
        desc = f"mapreduce pass {i}"
        sc.setJobDescription(desc)
        t = time.perf_counter()
        a, b = json.loads(sc.textFile(str(state)).first())          # read the last pass's output
        g = (sc.textFile(str(points_dir), 16)                        # map: read and parse every line
             .map(parse)
             .map(lambda p, a=a, b=b: (0, gradient(p, a, b)))
             .reduceByKey(add)                                        # shuffle, then reduce
             .values().first())
        a, b = step(a, b, g)
        state = wdir / f"pass-{i}"
        sc.parallelize([json.dumps([a, b])], 1).saveAsTextFile(str(state))   # write the output
        times.append(time.perf_counter() - t)
        io.append(stage_io(desc))
    return a, b, times, io


def rdd(passes):
    a = b = 0.0
    points = sc.textFile(str(points_dir), 16).map(parse).cache()
    times, io = [], []
    for i in range(1, passes + 1):
        desc = f"rdd pass {i}"
        sc.setJobDescription(desc)
        t = time.perf_counter()
        g = points.map(lambda p, a=a, b=b: gradient(p, a, b)).reduce(add)
        a, b = step(a, b, g)
        times.append(time.perf_counter() - t)
        io.append(stage_io(desc))
    return a, b, times, io, points


print(f"input: {n_files} text files, {in_bytes:,} bytes, {PASSES} passes, {sc.defaultParallelism} cores\n")
ma, mb, mt, mio = mapreduce(PASSES)
ra, rb, rt, rio, points = rdd(PASSES)

print(f"{'pass':>4}  {'mapreduce s':>11} {'input bytes':>13}  {'rdd s':>6} {'input bytes':>13}")
for i in range(PASSES):
    print(f"{i + 1:>4}  {mt[i]:>11.2f} {mio[i][0]:>13,}  {rt[i]:>6.2f} {rio[i][0]:>13,}")
print(f"{'sum':>4}  {sum(mt):>11.2f} {sum(x[0] for x in mio):>13,}  {sum(rt):>6.2f} {sum(x[0] for x in rio):>13,}")
print(f"\nmapreduce: shuffle write {sum(x[1] for x in mio):,} B, output files written {PASSES + 1}")
print(f"rdd:       shuffle write {sum(x[1] for x in rio):,} B")
print(f"\nmapreduce weights: a={ma:.6f} b={mb:.6f}")
print(f"rdd weights:       a={ra:.6f} b={rb:.6f}")
later = sorted(rt[1:])[len(rt[1:]) // 2]
later_mr = sorted(mt[1:])[len(mt[1:]) // 2]
print(f"\nmedian of passes 2..{PASSES}: mapreduce {later_mr:.2f} s, rdd {later:.2f} s "
      f"({later_mr / later:.1f}x)")

rdds = ui_api("storage/rdd")
for r in rdds:
    print(f"\ncached RDD {r['id']}: {r['numCachedPartitions']}/{r['numPartitions']} partitions, "
          f"{r['memoryUsed']:,} B in memory, {r['diskUsed']:,} B on disk")

print("\nlineage of the cached points (toDebugString):")
print(points.toDebugString().decode().replace(str(ROOT) + "/", ""))

print("\nwhat the mapreduce passes left on disk (first two of the state folders):")
for d in ("pass-0", "pass-1"):
    print(f"weights/{d}/")
    print("\n".join("  " + line for line in tree(w / "weights" / d).splitlines()))
print("\nthe input folder:")
print("\n".join("  " + line for line in tree(points_dir).splitlines()[:3]) + "\n  ...")
