"""Chapter 24: one table from the bench-<engine>-<spark>.json files that bench.py wrote.

Each engine is compared with the CPU on its own Spark version. A row hash that differs
from that CPU run is marked, so a fast wrong answer cannot hide in the table.
Run from code/:  uv run python ch24/bench_table.py
"""
import json
from pathlib import Path

OUT = Path(__file__).parent / "output"
runs = {(r["engine"], r["spark"]): r
        for r in (json.loads(p.read_text()) for p in sorted(OUT.glob("bench-*.json")))}
order = [("cpu", "4.3.0"), ("cpu", "4.2.0"), ("comet", "4.2.0"), ("rapids", "4.2.0"),
         ("auron", "4.2.0"), ("cpu", "4.1.1"), ("gluten", "4.1.1")]
order = [k for k in order if k in runs]
first = runs[order[0]]
print(f"{first['rows']:,} rows, median of {first['runs']} runs after {first['warmup']} warmup runs"
      " (seconds; x = speed-up against the CPU on the same Spark)\n")
queries = list(first["queries"])
print(f"{'engine':<8}{'spark':<7}" + "".join(f"{q:>16}" for q in queries))
for engine, spark in order:
    r, cpu = runs[(engine, spark)], runs.get(("cpu", spark))
    cells = []
    for q in queries:
        m = r["queries"][q]["median_s"]
        if engine == "cpu" or cpu is None:
            cells.append(f"{m:>16.2f}")
            continue
        same = r["queries"][q]["rows_hash"] == cpu["queries"][q]["rows_hash"]
        x = cpu["queries"][q]["median_s"] / m
        cells.append(f"{m:>8.2f} {x:>5.1f}x{'' if same else '!'}")
    print(f"{engine:<8}{spark:<7}" + "".join(cells))
print("\n! = the rows differ from the CPU rows on the same Spark version")
print("\npeak memory (MiB)")
print(f"{'engine':<8}{'spark':<7}{'JVM RSS':>9}{'heap':>7}{'off-heap exec':>15}{'GPU':>7}")
for engine, spark in order:
    m = runs[(engine, spark)]["memory"]
    gpu = m["gpu_peak_mib"] - m["gpu_idle_mib"] if engine == "rapids" else 0
    print(f"{engine:<8}{spark:<7}{m['jvm_peak_rss_mib']:>9}{m['jvm_heap_peak_mib']:>7}"
          f"{m['offheap_execution_peak_mib']:>15}{gpu:>7}")
