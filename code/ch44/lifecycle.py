"""Chapter 44, Step 6: the life of a session. Idle timeout, release, a server restart, and the
interruption of a running operation by its tag.

Run from code/:  uv run python ch44/lifecycle.py
"""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from connect_util import server, session, close, stop, rest, error_class  # noqa: E402
from pyspark.sql.connect import functions as F  # noqa: E402
from pyspark.sql.types import LongType  # noqa: E402


def run(label, fn):
    try:
        print(f"  {label:52} ok: {fn()}")
    except Exception as e:
        print(f"  {label:52} {error_class(e)}")


url = server("spark.connect.session.manager.defaultSessionTimeout=10s",
             "spark.connect.session.manager.maintenanceInterval=2s")
print("server: defaultSessionTimeout=10s, maintenanceInterval=2s")

a = session(url, user_id="idle")
run("a new session runs a query", lambda: a.range(3).count())
time.sleep(15)
run("the same session after 15 s without RPCs", lambda: a.range(3).count())

b = session(url, user_id="polite")
b.range(3).count()
b.client.release_session()
run("a session after release_session()", lambda: b.range(3).count())

c = session(url, user_id="restart")
c.range(3).count()
server("spark.connect.session.manager.defaultSessionTimeout=10s",
       "spark.connect.session.manager.maintenanceInterval=2s")
run("a session after the server restarted", lambda: c.range(3).count())


def nap(x):
    import time
    time.sleep(2)
    return x


d = session(url, user_id="cancel")
d.addTag("nightly")
q = d.range(0, 20, numPartitions=20).select(F.sum(F.udf(nap, LongType(), useArrow=False)("id")))
result = {}


def long_query():
    try:
        q.collect()
        result["outcome"] = "finished"
    except Exception as e:
        result["outcome"] = error_class(e)


t = threading.Thread(target=long_query)
t.start()
time.sleep(4)
interrupted = d.interruptTag("nightly")
t.join()
print(f"  {'interruptTag(nightly) during a 20-task query':52} interrupted {len(interrupted)} "
      f"operation(s); the query: {result['outcome']}")
op = next(o for o in rest("connect/operations") if o["operationId"] == interrupted[0])
print(f"  {'REST record of that operation':52} state {op['state']}, jobs {op['jobIds']}")
for s in (a, b, c, d):
    try:
        close(s)
    except Exception:
        pass
stop()
