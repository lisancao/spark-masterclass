"""The counter source of Chapter 65, as a module, so that plain Python tests can import it.

counter.py registers it. Its classes travel to the Python workers by value, because counter.py
calls pyspark.cloudpickle.register_pickle_by_value() on this module (Chapter 69 explains why).
"""
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition


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
