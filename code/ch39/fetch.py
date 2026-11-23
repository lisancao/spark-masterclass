"""fetchsize: time and the peak of the old generation of the heap for one task that reads the whole trips table.

  uv run python ch39/fetch.py          one child process per setting (a fresh JVM each time)
  uv run python ch39/fetch.py 0        the child, with fetchsize=0 (the driver keeps every row)
Run under the bench lock.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jdbc import PROPS, URL, pg_session  # noqa: E402

SETTINGS = ["0", "100", "default", "10000"]


def child(fs):
    spark = pg_session("ch39-fetch", {"spark.driver.memory": "6g",
                                      "spark.executor.metrics.pollingInterval": "100ms"})
    opts = {} if fs == "default" else {"fetchsize": fs}
    pools = [p for p in spark._jvm.java.lang.management.ManagementFactory.getMemoryPoolMXBeans()
             if str(p.getType()) == "Heap memory"]
    times = []
    for i in range(2):                                  # a warm-up read, then the measured read
        df = spark.read.jdbc(URL, "trips", properties={**PROPS, **opts})
        spark._jvm.java.lang.System.gc()
        for p in pools:
            p.resetPeakUsage()                          # measure the second read only
        t = time.perf_counter()
        df.write.format("noop").mode("overwrite").save()   # read every row, keep nothing
        times.append(time.perf_counter() - t)
    old = [p for p in pools if "Old" in p.getName()][0].getPeakUsage().getUsed()
    print("RESULT " + json.dumps({"fetchsize": fs, "s": round(times[-1], 2), "peak_old_mb": round(old / 2**20)}))


def main():
    print("one task reads all 3,837,248 rows of trips (22 columns) and drops them (noop sink)")
    print("driver memory 6g, local[4], G1; time of the second read; peak of the G1 old generation\n")
    print(f"{'fetchsize':>14} {'seconds':>8} {'old gen peak MiB':>17}")
    for fs in SETTINGS:
        r = subprocess.run([sys.executable, __file__, fs], capture_output=True, text=True)
        line = [x for x in r.stdout.splitlines() if x.startswith("RESULT ")]
        if not line:
            print(f"{fs:>14} failed: {r.stderr.strip().splitlines()[-1][:120] if r.stderr else '?'}")
            continue
        d = json.loads(line[0][7:])
        label = "1000 (default)" if fs == "default" else fs
        print(f"{label:>14} {d['s']:8.2f} {d['peak_old_mb']:17,}")


if __name__ == "__main__":
    child(sys.argv[1]) if len(sys.argv) > 1 else main()
