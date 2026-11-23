"""Chapter 71, Step 12 figure: the JVM function options. Numbers from code/ch71/output/jvm_bench.txt.
Run from code/:  uv run python ch71/figures_jvm.py [png-check-dir]"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None
A = [("native is_png_fast (StaticInvoke)", 0.215, 0.192, 0.277), ("built-in substring = X'89504E47...'", 0.186, 0.161, 0.195),
     ("Java UDF1", 0.160, 0.134, 0.241), ("Hive GenericUDF", 0.173, 0.157, 0.234)]
B = [("built-in avg (HashAggregate)", 0.134, 0.090, 0.147), ("Aggregator, udaf (ObjectHashAggregate)", 0.327, 0.311, 0.421),
     ("deprecated UDAF (HashAggregate)", 0.156, 0.137, 0.165), ("Hive GenericUDAFAverage (ObjectHashAggregate)", 0.333, 0.303, 0.369),
     ("pandas UDF, grouped aggregate", 0.388, 0.357, 0.445)]


def bench():
    f = Fig(900, 420, "Median seconds of each JVM function option, with the range of five runs")
    f.text(20, 28, "JVM function options, median of 5 runs after a warm-up (bench lock, local[*], 16 cores)",
           size=14, weight=700)
    X0, S = 380, 880

    def group(title, rows, y0, c_of):
        f.text(20, y0, title, size=13, weight=700)
        for i, (name, med, lo, hi) in enumerate(rows):
            y = y0 + 14 + i * 32
            f.text(X0 - 10, y + 15, name, size=12, anchor="end")
            f.rect(X0, y + 2, med * S, 18, c=c_of(name), fill=0.65, rx=2, stroke=0)
            f.line(X0 + lo * S, y + 11, X0 + hi * S, y + 11, c="ink", w=1.4)
            f.text(X0 + hi * S + 8, y + 15, f"{med:.3f} s", size=11, mono=True)
        return y0 + 14 + len(rows) * 32

    y = group("A. is_png over 20,000,000 cached values (filter, count)", A, 60, lambda n: "si")
    group("B. mean fare per pickup zone, 3,837,248 cached trips (group by, collect)", B, y + 30,
          lambda n: "go" if "Object" in n else ("pu" if "pandas" in n else "si"))
    f.line(X0, 50, X0, 405)
    f.text(880, 412, "bar: median, line: fastest to slowest run", size=11, c="mute", anchor="end", italic=True)
    return f.save("ch71-jvm-bench", CHECK)


if __name__ == "__main__":
    print(bench())
