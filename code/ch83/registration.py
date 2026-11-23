"""Chapter 83: a packaged source: auto-registration, registration rules, error conditions, worker logs.

Run by ch83/packaging.sh, with the Python of a virtual environment that has pyspark-mailbox installed.
  python ch83/registration.py classic        the lookup, the rules, the conditions and the logs
  python ch83/registration.py lookup         only: does format("mailbox") resolve, and by hand?
  python ch83/registration.py connect URL    the same source from a Spark Connect client
"""
import os, re, shutil, sys, tempfile, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
MAIL = HERE.parent / "data" / "messy" / "mail"
mode = sys.argv[1]


def condition(action):
    """(condition from getCondition(), first [CONDITION] in the message, exception type) or None."""
    try:
        action()
    except Exception as e:
        get = getattr(e, "getCondition", None)
        cond = get() if get else None
        m = re.search(r"\[([A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*)\]", str(e))
        return cond, (m.group(1) if m else None), type(e).__name__, str(e)
    return None


def last_error_line(text):
    hits = re.findall(r"^\s*(?:[\w.]+\.)?(\w+(?:Error|Exception): [^\n]*)", text, re.M)
    return (hits[-1] if hits else text.splitlines()[0])[:84]


def broken_folder():
    d = tempfile.mkdtemp(prefix="ch83-mail-")
    shutil.copy(MAIL / "ticket_0.eml", d)
    Path(d, "broken.eml").write_text("Subject: no sender\n\nhello\n")
    return d


if mode == "connect":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote(sys.argv[2]).getOrCreate()
else:
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("ch83")
             .config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4029")).getOrCreate())
    spark.sparkContext.setLogLevel("WARN" if os.environ.get("CH51_WARN") else "ERROR")

def shown(path):
    """A path with $CH51_VENVS and ~ in place of the long prefixes."""
    v = os.environ.get("CH51_VENVS")
    if v and path.startswith(v):
        path = "$CH51_VENVS" + path[len(v):]
    return path.replace(os.path.expanduser("~"), "~")


print("driver python:", shown(sys.executable))
print("PYSPARK_PYTHON:", shown(os.environ.get("PYSPARK_PYTHON") or "(not set)"))

# 1. The lookup: format("mailbox") with no register() call.
r = condition(lambda: print("format('mailbox'), no register():",
                            spark.read.format("mailbox").load(str(MAIL)).count(), "rows"))
if r:
    print("format('mailbox'), no register():", r[1] or r[2], "|", last_error_line(r[3]))

if mode in ("lookup", "connect"):
    # 2. Register by hand: the class is pickled by reference, so the workers must import the module.
    from pyspark_mailbox import MailboxDataSource
    spark.dataSource.register(MailboxDataSource)
    r = condition(lambda: print("register(MailboxDataSource), then count():",
                                spark.read.format("mailbox").load(str(MAIL)).count(), "rows"))
    if r:
        print("register(MailboxDataSource), then count():", r[1] or r[2], "\n  ", last_error_line(r[3]))
    if mode == "connect" and r:
        # 3. Send the package to the server session as a Python artifact.
        zdir = tempfile.mkdtemp(prefix="ch83-zip-")
        z = shutil.make_archive(os.path.join(zdir, "pyspark_mailbox"), "zip",
                                root_dir=str(HERE / "pyspark-mailbox" / "src"), base_dir="pyspark_mailbox")
        spark.addArtifacts(z, pyfile=True)
        r = condition(lambda: print("addArtifacts(pyspark_mailbox.zip, pyfile=True), then count():",
                                    spark.read.format("mailbox").load(str(MAIL)).count(), "rows"))
        if r:
            print("addArtifacts(pyspark_mailbox.zip, pyfile=True), then count():", r[1] or r[2],
                  "\n  ", last_error_line(r[3]))
        spark.dataSource.register(MailboxDataSource)
        print("register() again after addArtifacts(), then count():",
              spark.read.format("mailbox").load(str(MAIL)).count(), "rows")
        print("\nError conditions on Connect:")
        cases = [("unknown format", lambda: spark.read.format("nosuchformat").load().count()),
                 ("no path", lambda: spark.read.format("mailbox").load().count()),
                 ("read() raises", lambda: spark.read.format("mailbox").load(broken_folder()).count())]
        for label, action in cases:
            cond, in_msg, typ, _ = condition(action)
            print(f"  {label:<16} {typ:<28} getCondition(): {str(cond):<26} in message: {in_msg}")
    sys.exit(0)

from pyspark.sql.datasource import DataSource
from pyspark_mailbox import MailboxDataSource

# 4. Registration rules.
print("format('MailBox') (names have no case):", spark.read.format("MailBox").load(str(MAIL)).count(), "rows")


class Json(DataSource):
    @classmethod
    def name(cls):
        return "json"


r = condition(lambda: spark.dataSource.register(Json))
print("register a source named 'json':", r[0] or r[1])


class MailboxV2(MailboxDataSource):
    """A fix you want to try without a reinstall: same name, one more column."""
    def schema(self):
        return super().schema() + ", parsed_by string"

    def reader(self, schema):
        base = super().reader(schema)

        class R(type(base)):
            def read(self, partition):
                for row in super().read(partition):
                    yield row + ("MailboxV2",)
        r = R.__new__(R)
        r.__dict__.update(base.__dict__)
        return r


spark.dataSource.register(MailboxV2)
print("register() again under the same name:",
      spark.read.format("mailbox").load(str(MAIL)).select("parsed_by").distinct().first()[0])
spark.dataSource.register(MailboxDataSource)

# 5. Two error surfaces, and what getCondition() gives in a classic session.
print("\nError conditions in a classic session:")
cases = [("unknown format", lambda: spark.read.format("nosuchformat").load().count()),
         ("no path", lambda: spark.read.format("mailbox").load().count()),
         ("read() raises", lambda: spark.read.format("mailbox").load(broken_folder()).count())]
for label, action in cases:
    cond, in_msg, typ, text = condition(action)
    print(f"  {label:<16} {typ:<28} getCondition(): {str(cond):<26} in message: {in_msg}")
    print(f"  {'':<16} {last_error_line(text)}")

# 6. Worker logs (4.1): logging and print() in read(), collected into a table.
spark.conf.set("spark.sql.pyspark.worker.logging.enabled", "true")


class Chatty(MailboxDataSource):
    @classmethod
    def name(cls):
        return "chatty"

    def reader(self, schema):
        base = super().reader(schema)

        class R(type(base)):
            def read(self, partition):
                import logging
                logging.getLogger("chatty").info("info, level not set")     # dropped: WARNING is the default
                logging.getLogger("chatty").warning("files %s", partition.files)
                print("print with", len(partition.files), "files")           # three arguments
                yield from super().read(partition)
        r = R.__new__(R)
        r.__dict__.update(base.__dict__)
        return r


spark.dataSource.register(Chatty)
t0 = time.monotonic()
spark.read.format("chatty").option("partitions", 2).load(str(MAIL)).count()
from pyspark.sql import functions as F
logs = spark.tvf.python_worker_logs()
print("\npython_worker_logs() columns:", logs.columns)
first = logs.filter(F.col("logger").isin("chatty", "mailbox", "stdout")).count()
seen = [(0.0, first)]
while time.monotonic() - t0 < 20:
    n = logs.filter(F.col("logger").isin("chatty", "mailbox", "stdout")).count()
    if n != seen[-1][1]:
        seen.append((round(time.monotonic() - t0, 1), n))
    if n >= 12:
        break
    time.sleep(0.5)
print("rows from our loggers, read right after the action:", first, "| later reads:", seen[1:])
rows = (logs.filter(F.col("logger").isin("chatty", "mailbox", "stdout"))
        .select("level", "logger", "msg", F.col("context")["func_name"].alias("func"))
        .orderBy("logger", "msg").collect())
for r in rows:
    print(f"  {r.level:<8} {r.logger:<8} {r.func or '':<6} {r.msg[:56]!r}")
