"""Chapter 35 helper: a Classic session with a Hive metastore, so the metric views of one
script are there for the next script. The metastore and the warehouse are in $BOOK_DATA/work/part06/ch35.
"""
import logging
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import DATA, TRIPS, ZONES, session

WORK = DATA / "work" / "part06" / "ch35"
logging.getLogger("SQLQueryContextLogger").setLevel(logging.CRITICAL)


def mv_session(app):
    WORK.mkdir(parents=True, exist_ok=True)
    os.chdir(WORK)   # Derby writes metastore_db/ in the current folder
    return session(app, **{"spark.sql.catalogImplementation": "hive",
                           "spark.sql.warehouse.dir": str(WORK / "warehouse"),
                           "spark.sql.session.timeZone": "UTC",
                           "spark.sql.shuffle.partitions": "8"})


def attempt(spark, label, statement, show=False):
    """Run a statement and print 'accepted' or 'rejected' with the error condition."""
    try:
        df = spark.sql(statement)
        rows = df.collect()
        print(f"  accepted  {label}")
        if show:
            for r in rows:
                print("            " + " | ".join(str(v) for v in r))
    except Exception as e:
        cond = getattr(e, "getCondition", lambda: None)() or type(e).__name__
        if hasattr(e, "java_exception"):   # an error that has no error condition
            cond = e.java_exception.getClass().getName()
            msg = (e.java_exception.getMessage() or "").split("\n")[0][:100]
        else:
            msg = str(e).split("\n")[0]
            msg = msg.split("] ", 1)[-1][:100]
        print(f"  rejected  {label}\n            {cond}: {msg}")
