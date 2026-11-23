"""Chapter 18: Classic and Connect side by side, measured on one machine.

Start the server first:  ch18/server.sh start
Run from code/:          flock $BOOK_DATA/.bench.lock uv run python ch18/compare.py
Each mode runs in its own Python process, so that one cannot warm up the other.
"""
import json, os, signal, statistics, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import TRIPS

PORT = os.environ.get("CONNECT_PORT", "15002")
UI = os.environ.get("CONNECT_UI_PORT", "4041")


def rss_mb(pid):
    """Resident memory of a process and all of its children, in MB (from /proc)."""
    total, todo = 0, [pid]
    while todo:
        p = todo.pop()
        try:
            for line in open(f"/proc/{p}/status"):
                if line.startswith("VmRSS:"):
                    total += int(line.split()[1])
            todo += [int(c) for t in os.listdir(f"/proc/{p}/task")
                     for c in open(f"/proc/{p}/task/{t}/children").read().split()]
        except FileNotFoundError:
            pass
    return round(total / 1024)


def measure(mode):
    from pyspark.sql import SparkSession, functions as F
    t = time.perf_counter()
    if mode == "connect":
        spark = SparkSession.builder.remote(f"sc://localhost:{PORT}").create()
        spark.range(1).collect()                   # the session exists on the server after the first RPC
    else:
        spark = SparkSession.builder.master("local[*]").config("spark.ui.enabled", "false").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        spark.range(1).collect()
    start_s = time.perf_counter() - t

    trips = spark.read.parquet(str(TRIPS))
    top = trips.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(3)
    top.collect()                                  # warmup
    q = []
    for _ in range(10):
        t = time.perf_counter(); top.collect(); q.append(time.perf_counter() - t)
    s = []
    for i in range(20):                            # a new DataFrame each time, so no cached schema
        d = trips.withColumn(f"c{i}", F.lit(i))
        t = time.perf_counter(); d.columns; s.append(time.perf_counter() - t)

    where = None
    try:
        bad = trips.select("revenue")
    except Exception:
        where = "at select()"
    if where is None:
        try:
            bad.collect()
        except Exception:
            where = "at collect()"
    print(json.dumps({"start_s": round(start_s, 2), "query_ms": round(statistics.median(q) * 1000),
                      "schema_ms": round(statistics.median(s) * 1000, 1), "rss_mb": rss_mb(os.getpid()),
                      "error": where}))
    sys.stdout.flush()
    time.sleep(600)                                # wait here, so that the parent can kill this client


def semantics(mode):
    """Two behaviors that differ by mode: temp views and UDF closures (Spark docs, 'Eager vs Lazy')."""
    from pyspark.sql import SparkSession, functions as F
    if mode == "connect":
        spark = SparkSession.builder.remote(f"sc://localhost:{PORT}").create()
    else:
        spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    spark.range(3).createOrReplaceTempView("v")
    first = spark.table("v")
    spark.range(7).createOrReplaceTempView("v")
    x = 123
    f = F.udf(lambda: x, "int")
    df = spark.range(1).select(f().alias("x"))
    x = 456
    print(json.dumps({"view_rows": first.count(), "udf_value": df.first().x}))
    spark.stop()


if len(sys.argv) > 2:
    {"measure": measure, "semantics": semantics}[sys.argv[1]](sys.argv[2])
    sys.exit()

rows = {}
for mode in ["classic", "connect"]:
    p = subprocess.Popen([sys.executable, __file__, "measure", mode], stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, text=True)
    r = json.loads(p.stdout.readline())
    # Kill the client with SIGKILL, as an out-of-memory kill would. Does the driver survive?
    jvms_before = subprocess.run(["pgrep", "-P", str(p.pid)], capture_output=True, text=True).stdout.split()
    os.kill(p.pid, signal.SIGKILL); p.wait()
    time.sleep(5)
    if mode == "classic":
        alive = [j for j in jvms_before if Path(f"/proc/{j}").exists()]
        r["after_kill"] = f"driver JVM {'still runs' if alive else 'stopped'}"
    else:
        import urllib.request
        try:
            urllib.request.urlopen(f"http://localhost:{UI}/api/v1/applications", timeout=5)
            r["after_kill"] = "server still runs"
        except Exception:
            r["after_kill"] = "server stopped"
    s = subprocess.run([sys.executable, __file__, "semantics", mode], capture_output=True, text=True)
    r.update(json.loads(s.stdout.strip().splitlines()[-1]))
    rows[mode] = r

print(f"{'':34} {'Classic':>18} {'Connect':>18}")
labels = [("start_s", "first session and first query (s)"), ("query_ms", "top-3 zones query, median (ms)"),
          ("schema_ms", "df.columns, new DataFrame (ms)"), ("rss_mb", "client memory, RSS (MB)"),
          ("error", "bad column raises"), ("after_kill", "client killed with SIGKILL"),
          ("view_rows", "count of a replaced temp view"), ("udf_value", "UDF closure value")]
for k, label in labels:
    print(f"{label:34} {str(rows['classic'][k]):>18} {str(rows['connect'][k]):>18}")
