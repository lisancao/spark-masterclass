"""Chapter 55: three traps met while building casefile, each shown on Spark 4.3.0.

Run from code/:  uv run python ch55/gotchas.py
"""
import os, pickle, shutil, subprocess, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import casefile as cf  # noqa: E402
from common.book import session  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.datasource import DataSource, DataSourceReader  # noqa: E402

DATA = str(Path(__file__).resolve().parents[1] / "data" / "messy" / "casefiles")

print("== 1. partitions() that raises NotImplementedError")


class Placeholder(DataSource):
    @classmethod
    def name(cls):
        return "placeholder"

    def schema(self):
        return "partition string"

    def reader(self, schema):
        return PlaceholderReader()


class PlaceholderReader(DataSourceReader):
    def partitions(self):
        raise NotImplementedError        # a natural placeholder while you work

    def read(self, partition):
        yield (f"{type(partition).__name__}(value={partition.value!r})",)


spark = session("ch55-gotchas", spark_sql_session_timeZone="UTC")
spark.dataSource.register(Placeholder)
print("   rows:", [r.partition for r in spark.read.format("placeholder").load().collect()])

print("\n== 2. a Logger pickles by name")
import logging  # noqa: E402
log = logging.getLogger("casefile-demo")
log.addHandler(logging.NullHandler())
blob = pickle.dumps(log)
print("   pickled bytes name the logger only:", b"casefile-demo" in blob, f"({len(blob)} bytes)")
code = ("import pickle, sys, logging; l = pickle.loads(sys.stdin.buffer.read()); "
        "print('   in a new process: handlers =', l.handlers)")
out = subprocess.run([sys.executable, "-c", code], input=blob, capture_output=True)
print(out.stdout.decode().rstrip())

print("\n== 3. raise_error in the input does not test abort()")
spark.dataSource.register(cf.CaseFileSource)
out_dir = Path(tempfile.mkdtemp(prefix="casefile-abort-"))
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "2")
src = (spark.read.format("casefile").load(DATA).select("case_id", "status", "priority", "opened", "tags", "meta")
       .orderBy("case_id").coalesce(1))
bad = src.withColumn("status", F.when(F.col("case_id") == "CASE-0010", F.raise_error(F.lit("boom")))
                     .otherwise(F.col("status")))


class Spy(cf.CaseFileWriter):
    def abort(self, messages):
        pass                               # an abort() that does nothing


class SpySource(cf.CaseFileSource):
    @classmethod
    def name(cls):
        return "casefile_spy"

    def writer(self, schema, overwrite):
        return Spy(self.options, overwrite)


spark.dataSource.register(SpySource)
for label, df in (("raise_error on the last row", bad),
                  ("an invalid case_id on the last row",
                   src.withColumn("case_id", F.when(F.col("case_id") == "CASE-0010", F.lit("not a case"))
                                  .otherwise(F.col("case_id"))))):
    shutil.rmtree(out_dir, ignore_errors=True)
    try:
        df.write.format("casefile_spy").mode("append").save(str(out_dir))
    except Exception as e:
        kind = "USER_RAISED_EXCEPTION" if "USER_RAISED_EXCEPTION" in str(e) else "ValueError"
    left = sorted(os.listdir(out_dir)) if out_dir.exists() else []
    print(f"   {label:35} -> {kind:21} | job folders left by a no-op abort(): {len(left)}")
spark.stop()
