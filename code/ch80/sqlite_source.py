"""A Python data source over a SQLite table: key ranges like JDBC, value conversion of its own.

Chapter 80 reads with it; Chapter 81 subclasses it to add filter pushdown. The scripts that import
it call pyspark.cloudpickle.register_pickle_by_value(sqlite_source), so the workers get this code
by value and do not need to import the module.
"""
import datetime
import decimal
import pathlib
import re
import sqlite3

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition


def quote_ident(name):
    """A SQL identifier in double quotes. Names cannot be parameters, so quote them."""
    return '"' + name.replace('"', '""') + '"'


def connect_ro(path):
    """Open a SQLite file read-only."""
    return sqlite3.connect(pathlib.Path(path).resolve().as_uri() + "?mode=ro", uri=True)


def affinity(declared):
    """SQLite's affinity rules ("Datatypes In SQLite", section 3.1), in their order."""
    d = (declared or "").upper()
    if "INT" in d:
        return "INTEGER"
    if any(k in d for k in ("CHAR", "CLOB", "TEXT")):
        return "TEXT"
    if d == "" or "BLOB" in d:
        return "BLOB"
    if any(k in d for k in ("REAL", "FLOA", "DOUB")):
        return "REAL"
    return "NUMERIC"


def spark_type(declared):
    a, d = affinity(declared), (declared or "").upper()
    if a == "INTEGER":
        return "bigint"
    if a in ("TEXT", "BLOB"):
        return "string"
    if a == "REAL":
        return "double"
    m = re.match(r"(?:DECIMAL|NUMERIC)\s*\((\d+)\s*,\s*(\d+)\)", d)
    if m:
        return f"decimal({m.group(1)},{m.group(2)})"
    if d.startswith("BOOL"):
        return "boolean"
    if "DATE" in d or "TIME" in d:
        return "timestamp"
    return "double"


TRUE, FALSE = {"1", "true", "t", "yes", "y"}, {"0", "false", "f", "no", "n"}


def to_spark_value(v, t):
    """Convert one stored value to the column's Spark type, or None when it does not fit."""
    if v is None:
        return None
    if t == "bigint":
        return v if isinstance(v, int) and not isinstance(v, bool) else None
    if t == "double":
        return float(v) if isinstance(v, (int, float)) else None
    if t.startswith("decimal"):
        scale = int(t.split(",")[1].rstrip(")"))
        if isinstance(v, (int, float)):
            return decimal.Decimal(repr(v)).quantize(decimal.Decimal(1).scaleb(-scale))
        return None
    if t == "boolean":
        s = str(v).strip().lower()
        return True if s in TRUE else False if s in FALSE else None
    if t == "timestamp":
        if isinstance(v, int):
            return datetime.datetime.fromtimestamp(v, tz=datetime.timezone.utc)
        if isinstance(v, str):
            try:
                ts = datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                return None
            return ts if ts.tzinfo else ts.replace(tzinfo=datetime.timezone.utc)
        return None
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    return str(v)


def table_columns(conn, table):
    """(name, declared type) pairs, after a check that the table exists."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                        (table,)).fetchone():
        raise ValueError(f"No table named {table!r} in the database")
    return [(r[1], r[2]) for r in conn.execute(f"PRAGMA table_info({quote_ident(table)})")]


def range_partitions(column, split_points):
    """One (where_sql, params) per key range. The first range takes NULL keys, the last is open."""
    q = quote_ident(column)
    if not split_points:
        return [("1 = 1", [])]
    ranges = [(f"({q} < ? OR {q} IS NULL)", [split_points[0]])]
    for lo, hi in zip(split_points, split_points[1:]):
        ranges.append((f"({q} >= ? AND {q} < ?)", [lo, hi]))
    ranges.append((f"{q} >= ?", [split_points[-1]]))
    return ranges


def stride_bounds(lower, upper, n):
    """Even split points in key space between lower and upper, like JDBC's stride."""
    stride = (upper - lower) / n
    return sorted({int(lower + stride * i) for i in range(1, n)})


def quantile_bounds(conn, table, column, n):
    """Split points with the same number of rows on each side, from the index."""
    q, t = quote_ident(column), quote_ident(table)
    count = conn.execute(f"SELECT count({q}) FROM {t}").fetchone()[0]
    points = []
    for i in range(1, n):
        row = conn.execute(f"SELECT {q} FROM {t} WHERE {q} IS NOT NULL ORDER BY {q} "
                           f"LIMIT 1 OFFSET ?", (count * i // n,)).fetchone()
        if row and (not points or row[0] > points[-1]):
            points.append(row[0])
    return points


class SqlitePartition(InputPartition):
    def __init__(self, where, params, snapshot):
        self.where, self.params, self.snapshot = where, params, snapshot


class SqliteDataSource(DataSource):
    @classmethod
    def name(cls):
        return "sqlite"

    def __init__(self, options):
        super().__init__(options)
        for key in ("path", "table"):
            if key not in options:
                raise ValueError(f"option '{key}' is required")

    def schema(self):
        conn = connect_ro(self.options["path"])
        try:
            cols = table_columns(conn, self.options["table"])
        finally:
            conn.close()
        return ", ".join(f"`{n}` {spark_type(d)}" for n, d in cols)

    def reader(self, schema):
        return SqliteReader(self.options, schema)


class SqliteReader(DataSourceReader):
    def __init__(self, options, schema):
        self.path = str(pathlib.Path(options["path"]).resolve())
        self.table = options["table"]
        self.columns = [f.name for f in schema.fields]
        self.types = {f.name: f.dataType.simpleString() for f in schema.fields}
        self.key = options.get("partitionColumn")
        self.n = int(options.get("numPartitions", "1"))
        self.lower, self.upper = options.get("lowerBound"), options.get("upperBound")
        self.bounds = options.get("bounds", "stride")         # stride (like JDBC) or quantile
        self.pushed = []                                       # (sql, params), Chapter 81

    def split_points(self, conn):
        if not self.key or self.n <= 1:
            return []
        if self.bounds == "quantile":
            return quantile_bounds(conn, self.table, self.key, self.n)
        if self.lower is not None and self.upper is not None:
            lo, hi = int(self.lower), int(self.upper)
        else:
            q = quote_ident(self.key)
            lo, hi = conn.execute(f"SELECT min({q}), max({q}) FROM {quote_ident(self.table)}").fetchone()
        return stride_bounds(lo, hi, self.n)

    def partitions(self):
        conn = connect_ro(self.path)
        try:
            snapshot = conn.execute(f"SELECT max(rowid) FROM {quote_ident(self.table)}").fetchone()[0] or 0
            points = self.split_points(conn)
        finally:
            conn.close()
        return [SqlitePartition(w, p, snapshot) for w, p in range_partitions(self.key or "rowid", points)]

    def build_query(self, partition):
        conditions = [partition.where, "rowid <= ?"] + [sql for sql, _ in self.pushed]
        params = list(partition.params) + [partition.snapshot]
        for _, p in self.pushed:
            params += p
        cols = ", ".join(quote_ident(c) for c in self.columns)
        return (f"SELECT {cols} FROM {quote_ident(self.table)} WHERE " + " AND ".join(conditions),
                params)

    def read(self, partition):
        if not isinstance(partition, SqlitePartition):
            return
        sql, params = self.build_query(partition)
        conn = connect_ro(self.path)
        try:
            cursor = conn.execute(sql, params)
            while True:
                rows = cursor.fetchmany(500)
                if not rows:
                    break
                for row in rows:
                    yield tuple(to_spark_value(v, self.types[c]) for v, c in zip(row, self.columns))
        finally:
            conn.close()
