"""Chapter 19: make a sort spill, and read the spill metrics from the REST API.

The driver keeps the default 1g heap (in local mode the driver is the executor).
Run from code/:  flock ~/spark-masterclass/data/.bench.lock uv run python ch19/spill.py
"""
import statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, ui_api, TRIPS

spark = session("ch19-spill", spark_ui_showConsoleProgress="false")
trips = spark.read.parquet(str(TRIPS))       # June: 3,837,248 rows, 20 columns
print("heap:", spark._jvm.java.lang.Runtime.getRuntime().maxMemory() // 2**20, "MiB;",
      "storage+execution pool:", ui_api("executors")[0]["maxMemory"] // 2**20, "MiB")

CASES = [("2", "false"), ("16", "false"), ("200", "false"), ("2", "true"), ("200", "true")]
RUNS = 3
print(f"\nsort all June trips by trip_distance, write to noop; median of {RUNS} runs after a warm-up\n")
print(f"{'shuffle.partitions':>18}{'AQE':>6}{'sort tasks':>12}{'memory spilled':>16}{'disk spilled':>14}"
      f"{'shuffle write':>15}{'time':>8}")
for parts, aqe in CASES:
    spark.conf.set("spark.sql.shuffle.partitions", parts)
    spark.conf.set("spark.sql.adaptive.enabled", aqe)
    times = []
    for i in range(RUNS + 1):
        label = f"spill {parts} {aqe} {i}"
        spark.sparkContext.setJobDescription(label)
        t = time.perf_counter()
        trips.orderBy("trip_distance").write.format("noop").mode("overwrite").save()
        if i:
            times.append(time.perf_counter() - t)
    stages = [s for s in ui_api("stages?status=complete") if s.get("description") == label]
    sort = max(stages, key=lambda s: s["shuffleReadBytes"])           # the stage after the shuffle
    write = sum(s["shuffleWriteBytes"] for s in stages)
    print(f"{parts:>18}{aqe:>6}{sort['numTasks']:>12}{sort['memoryBytesSpilled'] / 2**20:>12,.0f} MiB"
          f"{sort['diskBytesSpilled'] / 2**20:>10,.0f} MiB{write / 2**20:>11,.0f} MiB{statistics.median(times):>7.1f}s")
