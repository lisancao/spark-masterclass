"""Chapter 66: what happens to the tuples a reader yields, Arrow batches instead, and the time-zone trap.

Run from code/:  uv run python ch66/arrow_batches.py          (conversion table and errors)
                 uv run python ch66/arrow_batches.py --timing (tuples vs Row vs RecordBatch)
                 uv run python ch66/arrow_batches.py --tz     (naive vs aware datetimes, per worker TZ)
"""
import datetime, decimal, os, re, statistics, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from common.book import session

UTC = datetime.timezone.utc
VALUES = {   # (declared type, Python value) pairs to probe, by name
    "int 7": ("int", 7), "int None": ("int", None), "int 1.5": ("int", 1.5),
    "int True": ("int", True), "int '7'": ("int", "7"), "int 2**40": ("int", 2 ** 40),
    "double 2": ("double", 2), "decimal(5,2) Decimal": ("decimal(5,2)", decimal.Decimal("1.25")),
    "decimal(5,2) 1.25": ("decimal(5,2)", 1.25), "date '2026-10-01'": ("date", "2026-10-01"),
    "timestamp str": ("timestamp", "2026-10-01 10:00:00"),
    "timestamp aware": ("timestamp", datetime.datetime(2026, 10, 1, 10, tzinfo=UTC)),
    "timestamp_ntz aware": ("timestamp_ntz", datetime.datetime(2026, 10, 1, 10, tzinfo=UTC)),
    "binary bytes": ("binary", b"\x00\x01"), "binary str": ("binary", "ab"),
    "array<int> list": ("array<int>", [1, 2]), "map<string,int> dict": ("map<string,int>", {"k": 1}),
    "struct dict": ("struct<a:int,b:string>", {"a": 1, "b": "x"}),
}


class Probe(DataSource):
    """One row, one column: the declared type and the value come from VALUES[option 'case']."""
    @classmethod
    def name(cls):
        return "probe"

    def schema(self):
        return f"v {VALUES[self.options['case']][0]}"

    def reader(self, schema):
        return ProbeReader(self.options)


class ProbeReader(DataSourceReader):
    def __init__(self, options):
        self.case = options["case"]

    def read(self, partition):
        yield (VALUES[self.case][1],)


class Rows(DataSource):
    """`rows` rows of (id int, sensor string, value double, ts timestamp) in 4 partitions."""
    @classmethod
    def name(cls):
        return "rows"

    def schema(self):
        return "id int, sensor string, value double, ts timestamp"

    def reader(self, schema):
        return RowsReader(self.options)


class Part(InputPartition):
    def __init__(self, start, end):
        self.start, self.end = start, end


class RowsReader(DataSourceReader):
    def __init__(self, options):
        self.rows = int(options["rows"])
        self.mode = options.get("mode", "tuple")

    def partitions(self):
        step = self.rows // 4
        return [Part(i * step, (i + 1) * step) for i in range(4)]

    def read(self, partition):
        base = datetime.datetime(2026, 10, 1, tzinfo=UTC)
        if self.mode == "tuple":
            for i in range(partition.start, partition.end):
                yield (i, f"s{i % 8}", i * 0.5, base + datetime.timedelta(seconds=i))
        elif self.mode == "row":
            from pyspark.sql import Row
            for i in range(partition.start, partition.end):
                yield Row(id=i, sensor=f"s{i % 8}", value=i * 0.5, ts=base + datetime.timedelta(seconds=i))
        else:
            import pyarrow as pa
            import pyarrow.compute as pc
            schema = pa.schema([("id", pa.int32()), ("sensor", pa.string()),
                                ("value", pa.float64()), ("ts", pa.timestamp("us", tz="UTC"))])
            if self.mode == "arrow_wrong":
                schema = schema.set(0, pa.field("id", pa.int64()))    # long, but the schema says int
            for lo in range(partition.start, partition.end, 10_000):
                ids = pa.array(range(lo, min(lo + 10_000, partition.end)), schema.field("id").type)
                yield pa.RecordBatch.from_arrays([
                    ids,
                    pc.binary_join_element_wise("s", pc.cast(pc.bit_wise_and(ids, 7), pa.string()), ""),
                    pc.multiply(pc.cast(ids, pa.float64()), 0.5),
                    pc.add(pa.scalar(base, schema.field("ts").type),
                           pc.cast(pc.multiply(pc.cast(ids, pa.int64()), 1_000_000), pa.duration("us"))),
                ], schema=schema)


def last_error(e):
    hits = [m.group(1) for l in str(e).splitlines()
            if (m := re.match(r"^\s*(?:[\w.]+\.)?(\w+(?:Error|Exception|Invalid): .*)", l))]
    if hits:
        return hits[-1][:66]
    if "AssertionError" in str(e):              # a bare assert in PySpark's converters
        where = re.findall(r'File ".*/(\w+\.py)", line \d+, in (\w+)', str(e))
        return f"AssertionError in {where[-1][0]}:{where[-1][1]}()" if where else "AssertionError"
    return str(e).splitlines()[0][:72]


if "--tz" in sys.argv:
    # Child mode: the session zone is UTC; the worker's zone is whatever TZ this process has.
    spark = session("ch66-tz", spark_sql_session_timeZone="UTC")

    class TzProbe(Probe):
        @classmethod
        def name(cls):
            return "tzprobe"

        def schema(self):
            return "naive timestamp, aware timestamp"

        def reader(self, schema):
            class R(DataSourceReader):
                def read(self, partition):
                    yield (datetime.datetime(2026, 10, 1, 10, 0),
                           datetime.datetime(2026, 10, 1, 10, 0, tzinfo=UTC))
            return R()

    spark.dataSource.register(TzProbe)
    r = spark.read.format("tzprobe").load().selectExpr("cast(naive as string)", "cast(aware as string)").first()
    print(f"  TZ={os.environ.get('TZ'):<20} naive -> {r[0]}   aware -> {r[1]}")
    sys.exit(0)

if "--timing" in sys.argv:
    spark = session("ch66-timing")
    spark.dataSource.register(Rows)
    n = 2_000_000
    results = {}
    for mode in ("tuple", "row", "arrow"):
        df = spark.read.format("rows").option("rows", n).option("mode", mode).load()
        df.selectExpr("sum(id)", "max(sensor)", "sum(value)", "max(ts)").collect()        # warm-up
        times = []
        for _ in range(5):
            t = time.perf_counter()
            df.selectExpr("sum(id)", "max(sensor)", "sum(value)", "max(ts)").collect()
            times.append(time.perf_counter() - t)
        results[mode] = statistics.median(times)
        print(f"{mode:<6} {n:,} rows: median {results[mode]:.2f} s over 5 runs "
              f"(min {min(times):.2f}, max {max(times):.2f})")
    print(f"tuples / batches: {results['tuple'] / results['arrow']:.1f}x, "
          f"rows / tuples: {results['row'] / results['tuple']:.1f}x")
    print(f"conditions: local[*] on {os.cpu_count()} cores, 4 partitions, "
          f"maxRecordsPerBatch {spark.conf.get('spark.sql.execution.arrow.maxRecordsPerBatch')}")
    sys.exit(0)

spark = session("ch66-arrow", spark_sql_session_timeZone="UTC")
spark.dataSource.register(Probe)
spark.dataSource.register(Rows)
print(f"{'declared, yielded':<22} result")
for case in VALUES:
    try:
        expr = "hex(v)" if VALUES[case][0] == "binary" else "cast(v as string)"
        v = spark.read.format("probe").option("case", case).load().selectExpr(expr).first()[0]
        print(f"{case:<22} {v}")
    except Exception as e:
        print(f"{case:<22} fails: {last_error(e)}")

print("\nRecordBatch with id as int64, schema says int:")
try:
    spark.read.format("rows").option("rows", 8).option("mode", "arrow_wrong").load().count()
except Exception as e:
    text = str(e)
    i = text.index("[DATA_SOURCE_RETURN_SCHEMA_MISMATCH]")
    import textwrap
    msg = " ".join(l.strip() for l in text[i:].split("columns. Make sure")[0].splitlines())
    print(textwrap.indent(textwrap.fill(msg + " columns. ...", 86), "  "))
print("the same rows, correct batches:", spark.read.format("rows").option("rows", 8).option("mode", "arrow")
      .load().orderBy("id").select("id", "sensor", "value").collect()[:2])

print("\nTimestamps: session time zone UTC, three worker time zones:")
for tz in ("UTC", "America/Los_Angeles", "Asia/Ho_Chi_Minh"):
    out = subprocess.run([sys.executable, __file__, "--tz"], capture_output=True, text=True,
                         env=dict(os.environ, TZ=tz))
    print(out.stdout.rstrip())
