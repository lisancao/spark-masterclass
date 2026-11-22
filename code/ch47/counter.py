"""Chapter 47: partitions, options, user schemas, SQL, and the moments when each error appears.

Run from code/:  uv run python ch47/counter.py
"""
import pickle, re, socket, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition, CaseInsensitiveDict
from pyspark.sql.types import LongType, StringType, StructField, StructType
from common.book import session

spark = session("ch47-counter")


def plan_ranges(rows, partitions):
    """Split range(rows) into at most `partitions` contiguous ranges, sizes differ by at most one."""
    n = min(partitions, rows)
    if n <= 0:
        return []
    base, extra = divmod(rows, n)
    ranges, start = [], 0
    for i in range(n):
        end = start + base + (1 if i < extra else 0)
        ranges.append((start, end))
        start = end
    return ranges


def whole_number(options, key, default, minimum):
    raw = options.get(key, default)
    if raw is None:
        raise ValueError(f"option '{key}' is required")
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"option '{key}' must be a whole number, got {raw!r}") from None
    if value < minimum:
        raise ValueError(f"option '{key}' must be >= {minimum}, got {value}")
    return value


class RangePartition(InputPartition):
    def __init__(self, start, end):
        self.start, self.end = start, end


class CounterDataSource(DataSource):
    @classmethod
    def name(cls):
        return "counter"

    def schema(self):
        return "id long, label string"

    def reader(self, schema):
        return CounterReader(self.options, schema)


class CounterReader(DataSourceReader):
    def __init__(self, options, schema):
        self.rows = whole_number(options, "rows", None, 0)
        self.partition_count = whole_number(options, "partitions", "2", 1)
        self.prefix = options.get("prefix", "row")
        self.columns = [f.name for f in schema.fields]
        unknown = set(self.columns) - {"id", "label"}
        if unknown:
            raise ValueError(f"counter has no column(s) {sorted(unknown)}")

    def partitions(self):
        return [RangePartition(s, e) for s, e in plan_ranges(self.rows, self.partition_count)]

    def read(self, partition):
        if not isinstance(partition, RangePartition):   # the InputPartition(None) Spark adds
            return
        for i in range(partition.start, partition.end):
            values = {"id": i, "label": f"{self.prefix}-{i}"}
            yield tuple(values[c] for c in self.columns)


print("plan_ranges:", plan_ranges(10, 3), plan_ranges(2, 5), plan_ranges(0, 2))
spark.dataSource.register(CounterDataSource)

df = spark.read.format("counter").option("rows", 10).option("partitions", 3).load()
(df.groupBy(F.spark_partition_id().alias("partition"))
   .agg(F.min("id").alias("first"), F.max("id").alias("last"), F.count("*").alias("rows"))
   .orderBy("partition").show())

print("rows=0 (no partitions planned):",
      spark.read.format("counter").option("rows", 0).load().rdd.getNumPartitions(), "partition,",
      spark.read.format("counter").option("rows", 0).load().count(), "rows")

print("\nA user schema, in another order:")
spark.read.format("counter").schema("label string, id long").option("rows", 2).load().show()

spark.sql("CREATE OR REPLACE TEMPORARY VIEW five USING counter OPTIONS (rows '5', prefix 'n')")
spark.sql("SELECT * FROM five WHERE id >= 3").show()


# Plain Python: the same reader with no Spark involved.
SCHEMA_STRUCT = StructType([StructField("id", LongType()), StructField("label", StringType())])
reader = CounterReader(CaseInsensitiveDict({"rows": "3", "partitions": "2"}), SCHEMA_STRUCT)
print("without Spark:", [row for p in reader.partitions() for row in reader.read(p)])
pickle.loads(pickle.dumps(reader))
print("the reader pickles\n")


# --- When each kind of error appears -------------------------------------------------
def condition(e):
    """The error condition: getCondition() when it is set, else the first [NAME] in the message."""
    get = getattr(e, "getCondition", None)
    name = get() if get else None
    m = re.search(r"\[([A-Z][A-Z_]+[A-Z])\]", str(e))
    return name or (m.group(1) if m else type(e).__name__)


def last_line(e):
    """The last Python exception line in the message, without the module path."""
    hits = [m for l in str(e).splitlines()
            if (m := re.match(r"^\s*(?:[\w.]+\.)?(\w+(?:Error|Exception|Invalid)): (.*)", l))]
    if not hits:
        return str(e).splitlines()[0][:86]
    name, msg = hits[-1].groups()
    return f"{name}: {msg}"[:86]


def attempt(label, build):
    stage = "build"
    try:
        df = build()
        stage = "action"
        df.collect()
        print(f"{label:<34} no error")
    except Exception as e:
        head = re.sub(r"^.*?\] ", "", str(e).splitlines()[0]).split(":")[0][:60]
        print(f"{label:<34} fails at {stage}: {condition(e)}\n{'':<4}{head}\n{'':<4}{last_line(e)}")


class BadSchema(CounterDataSource):
    @classmethod
    def name(cls):
        return "badschema"

    def schema(self):
        raise RuntimeError("schema() broke")


class GeneratorPartitions(CounterDataSource):
    @classmethod
    def name(cls):
        return "genparts"

    def reader(self, schema):
        r = CounterReader(self.options, schema)
        r.partitions = lambda: (RangePartition(0, 1) for _ in range(2))   # a generator, not a list
        return r


class SocketReader(CounterDataSource):
    @classmethod
    def name(cls):
        return "socketreader"

    def reader(self, schema):
        r = CounterReader(self.options, schema)
        r.conn = socket.socket()                   # a connection kept on self
        return r


class ShortRows(CounterDataSource):
    @classmethod
    def name(cls):
        return "shortrows"

    def reader(self, schema):
        class R(CounterReader):
            def read(self, partition):
                yield (1,)                         # one value for a two-column schema
        return R(self.options, schema)


class DictRows(ShortRows):
    @classmethod
    def name(cls):
        return "dictrows"

    def reader(self, schema):
        class R(CounterReader):
            def read(self, partition):
                yield {"id": 1, "label": "x"}
        return R(self.options, schema)


class FixedOrder(ShortRows):
    @classmethod
    def name(cls):
        return "fixedorder"

    def reader(self, schema):
        class R(CounterReader):
            def read(self, partition):
                if isinstance(partition, RangePartition):
                    for i in range(partition.start, partition.end):
                        yield (i, f"row-{i}")      # always (id, label), whatever the schema says
        return R(self.options, schema)


for cls in (FixedOrder, BadSchema, GeneratorPartitions, SocketReader, ShortRows, DictRows):
    spark.dataSource.register(cls)

print("When each kind of error appears:\n")
attempt("schema() raises", lambda: spark.read.format("badschema").option("rows", 3).load())
attempt("required option missing", lambda: spark.read.format("counter").load())
attempt("option is not a number", lambda: spark.read.format("counter").option("rows", "ten").load())
attempt("partitions() is a generator", lambda: spark.read.format("genparts").option("rows", 3).load())
attempt("socket kept on the reader", lambda: spark.read.format("socketreader").option("rows", 3).load())
attempt("row too short", lambda: spark.read.format("shortrows").option("rows", 3).load())
attempt("row is a dict", lambda: spark.read.format("dictrows").option("rows", 3).load())
attempt("schema asks for a column we lack",
        lambda: spark.read.format("counter").schema("id long, colour string").option("rows", 3).load())
attempt("user schema, fixed tuple order",
        lambda: spark.read.format("fixedorder").schema("label string, id long").option("rows", 3).load())
