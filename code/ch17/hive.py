"""Helpers for the Chapter 17 scripts: a session with Hive support in its own folder, and a
statement runner that prints each statement and its rows."""
import os
import re
import sys
import textwrap
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import work, ui_port  # noqa: E402,F401

UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def hive_session(name, folder, **conf):
    """A local session with enableHiveSupport(). The Derby metastore (metastore_db), derby.log and the
    warehouse all go into `folder`, which is also the working directory."""
    from pyspark.sql import SparkSession
    os.chdir(folder)
    b = (SparkSession.builder.master("local[2]").appName(name)
         .config("spark.ui.port", ui_port())
         .config("spark.sql.warehouse.dir", str(Path(folder) / "warehouse")))
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def error_text(e, width=88, lines=2):
    text = " ".join(l.strip() for l in str(e).splitlines() if l.strip()).split(" JVM stacktrace:")[0]
    wrapped = textwrap.wrap(f"{type(e).__name__}: {text}", width - 2)
    return "\n".join("  " + w for w in wrapped[:lines]) + (" ..." if len(wrapped) > lines else "")


def runner(spark):
    def q(sql, n=30, show=True, width=88):
        lines = [l.strip() for l in sql.strip().splitlines()]
        print("> " + "\n     ".join(lines))
        try:
            df = spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            print(error_text(e, width, 2))
            return None
        if show and df.columns:
            try:
                df.show(n, truncate=False)
            except Exception as e:  # noqa: BLE001
                print(error_text(e, width, 2))
        return df
    return q


def files(root, label=None, pattern="*", show_hidden=False):
    root = Path(root)
    if label:
        print(f"  {label}:")
    if not root.exists():
        print("    (no folder)")
        return
    out = [p for p in sorted(root.rglob(pattern)) if p.is_file()
           and (show_hidden or not (p.name.startswith(".") or p.name.endswith(".crc")))]
    if not out:
        print("    (empty)")
    for p in out:
        print(f"    {UUID.sub('<uuid>', str(p.relative_to(root)))}  ({p.stat().st_size} B)")


def describe(spark, table, keys):
    d = {}
    for r in spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect():
        d.setdefault(r.col_name, r.data_type)          # the first value (a partition comes before its table)
    return {k: d.get(k) for k in keys}
