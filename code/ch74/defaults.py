"""Chapter 74: the default value of each config in the reference table, from Spark 4.3.0 itself.

Spark keeps every registered config in a registry of ConfigEntry objects (core configs are
declared with ConfigBuilder, SQL configs with buildConf). This script asks that registry for
each key's default and the version that added it, so the table cannot drift from the jars.
Classic only (it reads JVM objects through Py4J).

Run from code/:  uv run python ch74/defaults.py > ch74/output/defaults.txt
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session

KEYS = """
spark.master spark.app.name spark.driver.memory spark.driver.cores spark.driver.maxResultSize
spark.executor.memory spark.executor.cores spark.executor.instances spark.executor.memoryOverhead
spark.memory.fraction spark.memory.storageFraction spark.default.parallelism
spark.dynamicAllocation.enabled spark.serializer spark.local.dir spark.ui.port
spark.eventLog.enabled spark.eventLog.dir spark.jars.packages spark.submit.pyFiles spark.archives
spark.sql.shuffle.partitions spark.sql.adaptive.enabled spark.sql.adaptive.coalescePartitions.enabled
spark.sql.adaptive.advisoryPartitionSizeInBytes spark.sql.adaptive.skewJoin.enabled
spark.sql.autoBroadcastJoinThreshold spark.sql.files.maxPartitionBytes spark.sql.ansi.enabled
spark.sql.session.timeZone spark.sql.caseSensitive spark.sql.execution.arrow.pyspark.enabled
spark.sql.execution.arrow.maxRecordsPerBatch spark.sql.execution.pythonUDF.arrow.enabled
spark.sql.parquet.compression.codec spark.sql.sources.partitionOverwriteMode
spark.sql.optimizer.singleTaskExecution.enabled spark.sql.warehouse.dir
spark.sql.catalogImplementation spark.sql.extensions spark.sql.defaultCatalog
spark.sql.streaming.checkpointLocation spark.sql.streaming.stateStore.providerClass
spark.connect.grpc.binding.port spark.sql.timestampType spark.sql.legacy.timeParserPolicy
""".split()

spark = session("ch74-defaults")
jvm = spark.sparkContext._jvm
# Load the objects that declare configs outside the core package object.
for obj in ["org.apache.spark.sql.internal.SQLConf$", "org.apache.spark.sql.internal.StaticSQLConf$",
            "org.apache.spark.sql.connect.config.Connect$"]:
    try:
        getattr(jvm, "java").lang.Class.forName(obj)
    except Exception as e:
        print("could not load", obj, type(e).__name__)

find = jvm.org.apache.spark.internal.config.ConfigEntry.findEntry
static_keys = set()
print(f"{'key':<50} {'default':<28} {'since':<6} {'static SQL'}")
for k in KEYS:
    e = find(k)
    if e is None:
        print(f"{k:<50} (not registered)")
        continue
    static = jvm.org.apache.spark.sql.internal.SQLConf.isStaticConfigKey(k)
    print(f"{k:<50} {str(e.defaultValueString()):<28} {e.version():<6} {static}")
spark.stop()
