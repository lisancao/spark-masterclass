"""Chapter 81: pushdown into SQLite: injection, semantic mismatches, and a guarded translation.

Run from code/:  uv run python ch81/sqlite_pushdown.py
"""
import decimal, json, os, sys, tempfile, uuid
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ch80"))

from pyspark import cloudpickle
from pyspark.sql.datasource import (EqualTo, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull,
                                    LessThan, LessThanOrEqual, StringStartsWith)
from common.book import session, plan
import shopdb, sqlite_source
from sqlite_source import SqliteDataSource, SqliteReader, quote_ident

cloudpickle.register_pickle_by_value(sqlite_source)
spark = session("ch81-sqlite", spark_sql_python_filterPushdown_enabled="true",
                spark_sql_session_timeZone="UTC")
db = os.path.join(tempfile.mkdtemp(prefix="ch81-shop-"), "shop.db")
shopdb.build(db)
audit = tempfile.mkdtemp(prefix="ch81-sql-")

OPS = {EqualTo: "=", GreaterThan: ">", GreaterThanOrEqual: ">=", LessThan: "<", LessThanOrEqual: "<="}


def naive_literal(f, types):
    """Version 1: paste the value into the SQL text. Never do this."""
    if type(f) in OPS and len(f.attribute) == 1:
        return f"{quote_ident(f.attribute[0])} {OPS[type(f)]} '{f.value}'", []
    return None


def param(v):
    return float(v) if isinstance(v, decimal.Decimal) else v     # sqlite3 cannot bind Decimal


def naive_params(f, types):
    """Version 2: bound parameters, so no injection. But SQLite compares in its own way."""
    a = getattr(f, "attribute", None)
    if a is None or len(a) != 1:
        return None
    q = quote_ident(a[0])
    if type(f) in OPS:
        return f"{q} {OPS[type(f)]} ?", [param(f.value)]
    if isinstance(f, IsNull):
        return f"{q} IS NULL", []
    if isinstance(f, IsNotNull):
        return f"{q} IS NOT NULL", []
    if isinstance(f, StringStartsWith):
        return f"{q} LIKE ?", [f.value + "%"]
    return None


def guard(column, t):
    """SQL that is true exactly when to_spark_value() keeps the stored value (else it becomes NULL)."""
    q = quote_ident(column)
    if t == "bigint":
        return f"typeof({q}) = 'integer'"
    if t == "double" or t.startswith("decimal"):
        return f"typeof({q}) IN ('integer', 'real')"
    return f"typeof({q}) = 'text'"


def guarded(f, types):
    """Version 3: push a filter only where SQLite gives the answer Spark would give."""
    a = getattr(f, "attribute", None)
    if a is None or len(a) != 1 or types.get(a[0]) not in ("bigint", "string", "double") \
            and not str(types.get(a[0])).startswith("decimal"):
        return None                    # booleans, timestamps, untyped columns: leave them to Spark
    col, t = a[0], types[a[0]]
    q, g = quote_ident(col), guard(col, t)
    if type(f) in OPS:
        return f"({g} AND {q} {OPS[type(f)]} ?)", [param(f.value)]
    if isinstance(f, In):
        vals = [param(v) for v in f.value if v is not None]
        return (f"({g} AND {q} IN ({', '.join('?' * len(vals))}))", vals) if vals else ("0", [])
    if isinstance(f, IsNull):
        return f"(NOT ({g}) OR {q} IS NULL)", []
    if isinstance(f, IsNotNull):
        return f"({g})", []
    if isinstance(f, StringStartsWith) and t == "string":
        return f"({g} AND substr({q}, 1, ?) = ?)", [len(f.value), f.value]   # LIKE ignores case
    return None


COMPILERS = {"literal": naive_literal, "params": naive_params, "guarded": guarded}


class PushingReader(SqliteReader):
    def __init__(self, options, schema):
        super().__init__(options, schema)
        self.compile = options.get("compile", "guarded")
        self.audit = options.get("audit_dir")

    def pushFilters(self, filters):
        for f in filters:
            done = COMPILERS[self.compile](f, self.types) if self.compile != "off" else None
            if done is None:
                yield f
            else:
                self.pushed.append(done)

    def read(self, partition):
        if self.audit and hasattr(partition, "where"):
            sql, params = self.build_query(partition)
            with open(os.path.join(self.audit, f"{uuid.uuid4().hex}.json"), "w") as f:
                json.dump([sql, params], f, default=str)
        yield from super().read(partition)


class PushingSource(SqliteDataSource):
    @classmethod
    def name(cls):
        return "sqlite"

    def reader(self, schema):
        return PushingReader(self.options, schema)


spark.dataSource.register(PushingSource)


def orders(compile="guarded"):
    return (spark.read.format("sqlite").option("path", db).option("table", "orders")
            .option("compile", compile).option("audit_dir", audit).load())


def count(where, compile):
    try:
        return str(orders(compile).where(where).count())
    except Exception as e:
        import re
        hits = re.findall(r"^(?:[\w.]+\.)?(\w+(?:Error|Exception): [^\n]*)", str(e), re.M)
        return "fails: " + (hits[-1] if hits else str(e).splitlines()[0])[:52]


# 1. Values pasted into SQL.
print("Version 1, values pasted into the SQL text:")
for name in ("Ana", "x' OR '1'='1", "O'Brien"):
    w = "customer = '" + name.replace("'", "''") + "'"
    print(f"  customer = {name!r:<16} Spark alone: {count(w, 'off'):>4}   pushed: {count(w, 'literal')}")

# 2. Bound parameters: safe, but SQLite's comparison rules are not Spark's.
CONDITIONS = ["amount > 100", "qty > 3", "status LIKE 'ship%'", "amount IS NULL", "qty IN (2, 4)",
              "customer = 'ana'", "placed_at >= '2026-09-15'"]
print(f"\n{'WHERE':<28} {'Spark alone':>11} {'v2 params':>10} {'v3 guarded':>11}")
for w in CONDITIONS:
    v2 = count(w, "params")
    if v2.startswith("fails"):
        print(f"{w:<28} {count(w, 'off'):>11} {'(below)':>10} {count(w, 'guarded'):>11}\n  v2 {v2}")
    else:
        print(f"{w:<28} {count(w, 'off'):>11} {v2:>10} {count(w, 'guarded'):>11}")

# 3. What SQLite has in those rows.
import sqlite3
c = sqlite3.connect(db)
print("\nWhy: stored values that Spark reads as NULL")
for col, t in (("amount", "('integer', 'real')"), ("qty", "('integer')")):
    rows = c.execute(f"SELECT {col}, typeof({col}), count(*) FROM orders WHERE typeof({col}) NOT IN {t} "
                     f"GROUP BY 1, 2 ORDER BY 3 DESC").fetchall()
    print(f"  {col}: {rows}")
print("  SQLite: 'N/A' > 100 is", c.execute("SELECT 'N/A' > 100").fetchone()[0],
      "| 'Shipped' LIKE 'ship%' is", c.execute("SELECT 'Shipped' LIKE 'ship%'").fetchone()[0])

# 4. The SQL a task ran, and the plan.
for f in Path(audit).glob("*.json"):
    f.unlink()
q = orders().where("status LIKE 'ship%' AND amount > 100 AND gift")
print("\nstatus LIKE 'ship%' AND amount > 100 AND gift:", q.count(), "rows")
sql, params = json.load(open(next(Path(audit).glob("*.json"))))
import textwrap
print("  one task's SQL:")
print(textwrap.fill(sql, 84, initial_indent="    ", subsequent_indent="    "))
print("  params:", params)
import re
print("  " + [re.sub(r"#\d+L?", "", l.strip()) for l in plan(q, "simple").splitlines() if "Filter" in l][0][:86])

# 5. The plan-reuse trap through a temporary view: every query on the view shares one relation.
orders().createOrReplaceTempView("orders_v")
print("\nA temporary view over the source, three queries:")
for sql in ("SELECT count(*) FROM orders_v WHERE status = 'shipped'",
            "SELECT count(*) FROM orders_v",
            "SELECT count(*) FROM orders_v WHERE qty > 3"):
    print(f"  {sql:<56} {spark.sql(sql).first()[0]}")
print("  the right answers:", [orders("off").where(w).count() for w in ("status = 'shipped'", "true", "qty > 3")])
