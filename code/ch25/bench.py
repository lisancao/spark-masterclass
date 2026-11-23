"""Chapter 25: a large result, collected over Classic (with and without Arrow) and over Connect.

Each mode runs in its own process. The Connect server needs the same driver memory as Classic:
  ch24/server.sh start --conf spark.driver.memory=4g
  flock $BOOK_DATA/.bench.lock uv run python ch25/bench.py
"""
import json, os, statistics, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import TRIPS

COLS = ["PULocationID", "DOLocationID", "trip_distance", "fare_amount", "tpep_pickup_datetime"]
ROWS = 1_000_000            # for collect(), which makes one Python Row per row
REPS = 5                     # after one warmup run


def run(mode):
    """One mode, in this process: time toPandas() of the whole month and collect() of ROWS rows."""
    from pyspark.sql import SparkSession
    if mode == "connect":
        spark = SparkSession.builder.remote(f"sc://localhost:{os.environ.get('CONNECT_PORT', '15002')}").create()
    else:
        spark = (SparkSession.builder.master("local[*]").config("spark.driver.memory", "4g")
                 .config("spark.ui.enabled", "false")
                 .config("spark.sql.execution.arrow.pyspark.enabled", str(mode == "classic").lower())
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(str(TRIPS)).select(*COLS)
    out = {}
    for name, action in [("toPandas", lambda: df.toPandas()),
                         ("collect", lambda: df.limit(ROWS).collect())]:
        times = []
        for i in range(REPS + 1):
            t = time.perf_counter()
            result = action()
            if i:                                   # the first run is the warmup
                times.append(time.perf_counter() - t)
        out[name] = {"rows": len(result), "median_s": round(statistics.median(times), 2),
                     "min_s": round(min(times), 2), "max_s": round(max(times), 2)}
    spark.stop()
    print(json.dumps(out))


if len(sys.argv) > 1:
    run(sys.argv[1])
    sys.exit()

results = {}
for mode in ["classic-no-arrow", "classic", "connect"]:
    p = subprocess.run([sys.executable, __file__, mode], capture_output=True, text=True)
    results[mode] = json.loads(p.stdout.strip().splitlines()[-1])

print(f"{REPS} runs per cell after a warmup, median seconds (min-max)\n")
print(f"{'mode':18} {'toPandas, all rows':>26} {'collect, 1,000,000 rows':>26}")
for mode, r in results.items():
    cells = [f"{r[k]['median_s']:.2f} ({r[k]['min_s']:.2f}-{r[k]['max_s']:.2f})" for k in ("toPandas", "collect")]
    print(f"{mode:18} {cells[0]:>26} {cells[1]:>26}")
print(f"\nrows: toPandas {results['connect']['toPandas']['rows']:,}, collect {results['connect']['collect']['rows']:,}")
