"""Chapter 44: Python UDTFs: lifecycle, the Arrow choice, lateral joins, TABLE arguments and analyze().

Classic. Run from code/:  uv run python ch44/udtf_core.py [section ...]
Sections: lifecycle arrow convert lateral table analyze arrowudtf
"""
import json, os, shlex, shutil, sys, warnings
from datetime import datetime, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

warnings.filterwarnings("ignore")
from pyspark.sql import Row, functions as F
from pyspark.sql.functions import udtf
from pyspark.sql.types import IntegerType, StringType, StructType, TimestampType
from pyspark.sql.udtf import AnalyzeArgument, AnalyzeResult, OrderingColumn, PartitioningColumn
from common.book import session, plan, ROOT

LOG = ROOT / "code" / "data" / "service_logs" / "kv.log"
WANT = set(sys.argv[1:]) or {"lifecycle", "arrow", "convert", "lateral", "table", "analyze", "arrowudtf"}


def section(name, title):
    if name in WANT:
        print(f"\n=== {title}")
        return True
    return False


# The decision to use Arrow is made when udtf() runs, from the active session. Define one before a session.
@udtf(returnType="n int")
class BeforeSession:
    def eval(self, x: int):
        yield (x,)


spark = session("ch44-udtf", spark_sql_session_timeZone="UTC", spark_ui_showConsoleProgress="false")
lines = spark.read.text(str(LOG)).withColumnRenamed("value", "line")
NODES = ("EvalPythonUDTF", "Generate", "Exchange", "Sort ", "Join", "Window")
nodes = lambda df: [l.strip().lstrip("+-: ")[:96] for l in plan(df, "simple").splitlines() if any(n in l for n in NODES)]

# ---------------------------------------------------------------- lifecycle
if section("lifecycle", "lifecycle: which methods run, in which process, how often"):
    TRACE = Path("/tmp/masterclass-ch44/trace")
    shutil.rmtree(TRACE, ignore_errors=True); TRACE.mkdir(parents=True)

    @udtf(returnType="word string, n int")
    class Words:
        def __init__(self):
            self.id = f"{os.getpid()}-{id(self)}"
            self.calls = 0
            self.log("__init__")

        def log(self, what):
            with open(TRACE / f"{self.id}.log", "a") as f:
                f.write(what + "\n")

        def eval(self, text: str):
            self.calls += 1
            for w in text.split():
                yield w, self.calls

        def terminate(self):
            self.log(f"terminate after {self.calls} eval calls")
            yield "<end>", self.calls          # rows from terminate() come after the last eval

        def cleanup(self):
            self.log("cleanup")

    docs = spark.range(6, numPartitions=3).select(F.concat(F.lit("doc "), "id", F.lit(" has words")).alias("text"))
    rows = docs.lateralJoin(Words(F.col("text").outer())).collect()
    print(f"{len(rows)} rows from 6 input rows in 3 partitions")
    for f in sorted(TRACE.iterdir()):
        print(f"  instance {f.stem.split('-')[0]}-...: " + " | ".join(f.read_text().splitlines()))
    print("  rows from terminate():", sorted((r.text, r.word, r.n) for r in rows if r.word == "<end>"))
    # The same UDTF when one input partition is empty.
    sparse = spark.range(2, numPartitions=3).select(F.concat(F.lit("doc "), "id").alias("text"))
    print("  rows per partition of a second input:", [len(p) for p in sparse.rdd.glom().collect()])
    try:
        sparse.lateralJoin(Words(F.col("text").outer())).collect()
        print("  lateral join with terminate(): ran")
    except Exception as e:
        import re
        m = re.search(r"java\.lang\.\w+", str(e))
        print("  lateral join with terminate(), one empty partition ->", m.group(0) if m else str(e)[:80])

    @udtf(returnType="rows int")
    class CountRows:                 # a TABLE argument: eval gets a Row, terminate() has no input row to attach to
        def __init__(self):
            self.n = 0

        def eval(self, row: Row):
            self.n += 1

        def terminate(self):
            yield (self.n,)

    import re
    for label, arg in [("TABLE argument", sparse.asTable()),
                       ("TABLE argument, withSinglePartition()", sparse.asTable().withSinglePartition()),
                       ("TABLE argument, partitionBy('text')", sparse.asTable().partitionBy("text"))]:
        try:
            print(f"  {label:<40}terminate() rows:", sorted(r.rows for r in CountRows(arg).collect()))
        except Exception as e:
            m = re.search(r"java\.lang\.\w+", str(e))
            print(f"  {label:<40}->", m.group(0) if m else str(e)[:80])

# ---------------------------------------------------------------- arrow
if section("arrow", "Arrow or not: decided when udtf() runs"):
    print("spark.sql.execution.pythonUDTF.arrow.enabled =", spark.conf.get("spark.sql.execution.pythonUDTF.arrow.enabled"))

    @udtf(returnType="n int")
    class AfterSession:
        def eval(self, x: int):
            yield (x,)

    PlainOff = udtf(AfterSession.func, returnType="n int", useArrow=False)
    for name, f in [("defined before the session", BeforeSession), ("defined after the session", AfterSession),
                    ("useArrow=False", PlainOff)]:
        print(f"  {name:<28}", nodes(f(F.lit(1)))[0])

# ---------------------------------------------------------------- convert
if section("convert", "what a yielded value becomes"):
    CASES = [("n int", (7.9,)), ("n int", ("7",)), ("n int", (True,)), ("s string", (42,)),
             ("n int", 5), ("a int, b int", {"b": 2, "a": 1}), ("n int", None), ("n int", [3])]
    print(f"  {'schema':<14}{'yield':<20}{'Arrow (default)':<40}useArrow=False")
    for schema, value in CASES:
        out = []
        for arrow in (True, False):
            class T:
                def eval(self, v=value):
                    yield v
            try:
                r = udtf(T, returnType=schema, useArrow=arrow)().collect()
                out.append(str([tuple(x) for x in r]))
            except Exception as e:
                import re
                cls = re.findall(r"\[([A-Z][A-Z_.]+)\]", str(e))
                py = re.findall(r"(\w+(?:Error|Invalid)): ", str(e))
                out.append("error " + (cls[0] if cls else (py[-1] if py else type(e).__name__)))
        print(f"  {schema:<14}{value!r:<20}{out[0]:<40}{out[1]}")

# ---------------------------------------------------------------- lateral
if section("lateral", "scalar arguments and lateral joins"):
    @udtf(returnType="key string, value string")
    class Pairs:
        def eval(self, line: str):
            if line and "=" in line:
                for tok in line.split():
                    k, _, v = tok.partition("=")
                    if v:
                        yield k, v

    spark.udtf.register("pairs", Pairs)
    lines.createOrReplaceTempView("lines")
    df = lines.lateralJoin(Pairs(F.col("line").outer()))
    print("DataFrame lateralJoin:", df.count(), "rows")
    print("  " + "\n  ".join(nodes(df)))
    print("SQL LATERAL:", spark.sql("SELECT l.line, p.* FROM lines l, LATERAL pairs(l.line) p").count(), "rows")
    blank = lines.where(F.trim("line") == "").count()
    print(f"input lines {lines.count()}, of them blank {blank}")
    left = spark.sql("SELECT l.line, p.key FROM lines l LEFT JOIN LATERAL pairs(l.line) p")
    left_on = spark.sql("SELECT l.line, p.key FROM lines l LEFT JOIN LATERAL pairs(l.line) p ON true")
    print("LEFT JOIN LATERAL, no ON:   rows with no pair:", left.where("key IS NULL").count())
    print("LEFT JOIN LATERAL ... ON true: rows with no pair:", left_on.where("key IS NULL").count())
    print("  plan with ON true:\n  " + "\n  ".join(nodes(left_on)))

# ---------------------------------------------------------------- table
if section("table", "TABLE arguments: PARTITION BY and ORDER BY"):
    @udtf(returnType="user string, start timestamp, events int, pid int")
    class Sessions:
        def __init__(self):
            self.user, self.start, self.last, self.n = None, None, None, 0

        def eval(self, row: Row, gap_minutes: int):
            ts = row["ts"]
            if self.last is not None and ts - self.last > timedelta(minutes=gap_minutes):
                yield self.user, self.start, self.n, os.getpid()
                self.start, self.n = ts, 0
            self.user, self.start, self.last = row["user"], self.start or ts, ts
            self.n += 1

        def terminate(self):
            if self.n:
                yield self.user, self.start, self.n, os.getpid()

    spark.udtf.register("sessions", Sessions)
    events = (lines.select(F.regexp_extract("line", r"ts=(\S+)", 1).alias("ts_s"),
                           F.regexp_extract("line", r"(?i)user=(\w+)", 1).alias("user"))
              .where("user != ''").select(F.to_timestamp("ts_s").alias("ts"), "user"))
    events.createOrReplaceTempView("events")
    q = spark.sql("SELECT * FROM sessions(TABLE(events) PARTITION BY user ORDER BY ts, gap_minutes => 30)")
    rows = q.orderBy("user", "start").collect()
    print(f"{len(rows)} sessions; per user:", {u: sum(1 for r in rows if r.user == u) for u in sorted({r.user for r in rows})})
    print("  " + "\n  ".join(nodes(q)))
    df = Sessions(events.asTable().partitionBy("user").orderBy("ts"), F.lit(30))
    print("DataFrame asTable().partitionBy().orderBy():", df.count(), "sessions")
    try:
        spark.sql("SELECT * FROM sessions(TABLE(events) PARTITION BY user ORDER BY ts, 30)").count()
    except Exception as e:
        import re
        cls = re.findall(r"\[([A-Z][A-Z_0-9.]+)\]", str(e))
        print("ORDER BY ts, 30 ->", cls[0] if cls else str(e).strip().splitlines()[0][:96])

# ---------------------------------------------------------------- analyze
if section("analyze", "analyze(): the output schema from a constant argument"):
    TYPES = {"string": StringType(), "int": IntegerType(), "timestamp": TimestampType()}

    class KV:
        @staticmethod
        def analyze(line: AnalyzeArgument, spec: AnalyzeArgument) -> AnalyzeResult:
            if not spec.isConstantExpression or spec.value is None:
                raise ValueError("spec must be a constant string such as 'ts:timestamp,user'")
            schema = StructType()
            for part in spec.value.split(","):
                name, _, kind = part.partition(":")
                schema = schema.add(name.strip(), TYPES[kind.strip() or "string"])
            return AnalyzeResult(schema.add("_error", StringType()))

        def eval(self, line: str, spec: str):
            fields = [(p.partition(":")[0].strip(), p.partition(":")[2].strip() or "string") for p in spec.split(",")]
            if not line or not line.strip() or line.lstrip().startswith("#"):
                return
            try:
                pairs = {k.lower(): v for k, _, v in (t.partition("=") for t in shlex.split(line))}
            except ValueError as e:
                yield tuple([None] * len(fields)) + (f"unparseable: {e}",)
                return
            out, errors = [], []
            for name, kind in fields:
                v = pairs.get(name)
                try:
                    out.append(None if v is None else int(v) if kind == "int" else
                               datetime.fromisoformat(v.replace("Z", "+00:00")) if kind == "timestamp" else v)
                except ValueError:
                    out.append(None); errors.append(f"{name}={v!r}")
            yield tuple(out) + ("; ".join(errors) or None,)

    kv = udtf(KV)
    spark.udtf.register("kv", kv)
    q = spark.sql("SELECT p.* FROM lines, LATERAL kv(line, 'ts:timestamp,user,action,ms:int') p")
    print("schema from the spec:", q.schema.simpleString())
    print("rows:", q.count(), "| with an _error:", [tuple(r) for r in q.where("_error IS NOT NULL").select("user", "_error").collect()])
    try:
        spark.sql("SELECT p.* FROM lines, LATERAL kv(line, line) p").count()
    except Exception as e:
        cls = next((w.strip("[].") for w in str(e).split() if w.startswith("[")), "")
        print("a spec that is not constant ->", cls)

# ---------------------------------------------------------------- arrowudtf
if section("arrowudtf", "arrow_udtf (4.1+): batches in, batches out"):
    import pyarrow as pa, pyarrow.compute as pc
    from pyspark.sql.functions import arrow_udtf

    @arrow_udtf(returnType="user string, ms int")
    class SlowRequests:
        def eval(self, batch: pa.RecordBatch):
            ms = pc.cast(pc.extract_regex(batch["line"], r"ms=(?P<ms>\d+)").field("ms"), pa.int32(), safe=False)
            user = pc.extract_regex(batch["line"], r"user=(?P<u>\w+)").field("u")
            keep = pc.fill_null(pc.greater(ms, 800), False)
            yield pa.table({"user": pc.filter(user, keep), "ms": pc.filter(ms, keep)})

    q = SlowRequests(lines.where(F.col("line").rlike(r"user=\w+ .*ms=\d+")).asTable())
    print("requests over 800 ms:", sorted(tuple(r) for r in q.collect()))
    print("  " + "\n  ".join(nodes(q)))
    spark.udtf.register("slow", SlowRequests)
    try:
        spark.sql("SELECT * FROM lines, LATERAL slow(line)").count()
    except Exception as e:
        print("in a lateral join ->", next((w.strip("[].") for w in str(e).split() if w.startswith("[")), str(e)[:80]))
