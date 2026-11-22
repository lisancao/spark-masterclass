"""Chapter 24: one table from the bench-<engine>-<spark>.json files that bench.py wrote.

The bench ran two times (output/pass1/ and output/), on a machine that other jobs also
used. The table gives the median of each pass, and the speed-up of each engine against
the CPU on its own Spark version, for each pass. A row hash that
differs from that CPU run is marked, so a fast wrong answer cannot hide in the table.
Run from code/:  uv run python ch24/bench_table.py
"""
import json
from pathlib import Path

OUT = Path(__file__).parent / "output"


def load(d):
    return {(r["engine"], r["spark"]): r
            for r in (json.loads(p.read_text()) for p in sorted(d.glob("bench-*.json")))}


passes = [load(OUT / "pass1"), load(OUT)]
order = [("cpu", "4.3.0"), ("cpu", "4.2.0"), ("comet", "4.2.0"), ("rapids", "4.2.0"),
         ("auron", "4.2.0"), ("cpu", "4.1.1"), ("gluten", "4.1.1")]
order = [k for k in order if all(k in p for p in passes)]
first = passes[0][order[0]]
queries = list(first["queries"])
print(f"{first['rows']:,} rows; each cell: median of pass 1 / median of pass 2, in seconds")
print(f"({first['runs']} timed runs after {first['warmup']} warmup run in each pass);"
      " x = speed-up against the CPU on the same Spark, pass 1/pass 2\n")
print(f"{'engine':<7}{'spark':<6}" + "".join(f"{q:>24}" for q in queries))


def med(k, q):
    return [p[k]["queries"][q]["median_s"] for p in passes]


for engine, spark in order:
    cells = []
    for q in queries:
        a, b = med((engine, spark), q)
        cell = f"{a:.2f}/{b:.2f}"
        if engine != "cpu" and ("cpu", spark) in passes[0]:
            ca, cb = med(("cpu", spark), q)
            same = all(p[(engine, spark)]["queries"][q]["rows_hash"] ==
                       p[("cpu", spark)]["queries"][q]["rows_hash"] for p in passes)
            cell += f" {ca / a:.1f}/{cb / b:.1f}x{'' if same else '!'}"
        else:
            cell += " " * 10
        cells.append(f"{cell:>24}")
    print(f"{engine:<7}{spark:<6}" + "".join(cells))
print("\n! = the rows differ from the CPU rows on the same Spark version (see compare-*.txt)")
print("\npeak memory in pass 2 (MiB)")
print(f"{'engine':<7}{'spark':<6}{'JVM RSS':>9}{'JVM heap':>10}{'GPU pool':>10}")
for engine, spark in order:
    m = passes[1][(engine, spark)]["memory"]
    gpu = m["gpu_peak_mib"] - m["gpu_idle_mib"] if engine == "rapids" else 0
    print(f"{engine:<7}{spark:<6}{m['jvm_peak_rss_mib']:>9}{m['jvm_heap_peak_mib']:>10}{gpu:>10}")
