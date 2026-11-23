"""Chapter 75 figures. Numbers from code/ch75/output/pytest.txt; the tool table lists what ran in code/ch75.
Run from code/:  uv run python ch75/figures.py [png-check-dir]"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def pytest_runs():
    runs = [("MODE=classic (local[2])", 6.71, 3.56, 12.80),
            ("MODE=connect, .remote(\"local[2]\")", 5.22, 3.63, 11.40),
            ("MODE=connect, sc://localhost:15118", 0.0, 1.39, 2.16)]
    f = Fig(900, 250, "Ten tests with three kinds of session: where the time goes")
    f.text(20, 28, "The same 10 tests, three kinds of session (one run each, seconds)", size=14, weight=700)
    for i, (lab, c) in enumerate((("session start (setup)", "go"), ("first UDF call", "pu"), ("the rest", "si"))):
        f.rect(420 + i * 160, 42, 14, 14, c=c, fill=0.7, rx=2, stroke=0)
        f.text(440 + i * 160, 54, lab, size=12)
    X0, S = 300, 42
    for i, (name, setup, udf, total) in enumerate(runs):
        y = 85 + i * 52
        f.text(X0 - 10, y + 17, name, size=12, anchor="end", mono=True)
        x = X0
        for v, c in ((setup, "go"), (udf, "pu"), (total - setup - udf, "si")):
            if v > 0:
                f.rect(x, y, v * S, 24, c=c, fill=0.7, rx=2, stroke=0)
            x += v * S
        f.text(x + 8, y + 17, f"{total:.2f} s", size=12, mono=True, weight=700)
    f.text(20, 238, "From pytest --durations: setup is the session fixture, the first UDF call starts the Python workers.",
           size=12, c="mute")
    return f.save("ch75-pytest", CHECK)


def tools():
    rows = [("a UDF raises an exception", "PythonException, simplified traceback", "default (simplifiedTraceback)"),
            ("which row was bad?", "local variables in the traceback", "udf.tracebackWithLocals.enabled (4.3)"),
            ("the worker crashed", "stack at the fatal signal", "udf.faulthandler.enabled"),
            ("the worker hangs", "stack every N s in stderr", "udf.tracebackDumpIntervalSeconds (4.1)"),
            ("print() and logging", "rows in a table", "pyspark.worker.logging.enabled (4.1)"),
            ("a JVM error", "JVM stack trace in the message", "pyspark.jvmStacktrace.enabled"),
            ("step through the UDF", "pdb over TCP", "breakpoint() + remote-pdb, local[1]"),
            ("the UDF is slow", "per-UDF profile", "pyspark.udf.profiler (Chapter 74)")]
    f = Fig(900, 320, "Debugging a Python worker: symptom, what you get, and the setting")
    f.text(20, 28, "Debugging a Python worker: from the symptom to the setting", size=14, weight=700)
    f.table(20, 44, [230, 300, 340], rows, head=["symptom", "what you get", "setting (spark.sql.execution.pyspark. ...)"],
            size=12, rowh=28, mono=False, hi={1: "go", 3: "go", 4: "go"})
    f.text(20, 310, "Highlighted rows: settings added in 4.1 or 4.3. Some keys start with spark.sql.pyspark. (see the text).",
           size=11, c="mute")
    return f.save("ch75-tools", CHECK)


if __name__ == "__main__":
    for fn in (pytest_runs, tools):
        print(fn())
