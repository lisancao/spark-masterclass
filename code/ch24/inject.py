"""Chapter 24: what one config line (spark.plugins) puts into a session.

Starts one engine, then prints what its plugin changed: the session extensions, the
columnar rules and query-stage rules that the extensions injected, the shuffle manager,
and the memory settings. Run with the Python of the engine's Spark version, for example:
  ~/spark-masterclass/data/ch24/venv-4.2.0/bin/python ch24/inject.py comet
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ch24.engines import start, OFFLOAD_FRIENDLY

engine = sys.argv[1]
spark = start(engine, **OFFLOAD_FRIENDLY)
jss = spark._jsparkSession
state = jss.sessionState()
conf = spark.sparkContext.getConf()


def short(name):
    return name.split(".")[-1].split("$")[0] or name


def seq(s):
    return [s.apply(i) for i in range(s.size())]


print(f"{engine} on Spark {spark.version}")
print(f"  spark.plugins          = {conf.get('spark.plugins', '(not set)')}")
exts = conf.get("spark.sql.extensions", "(not set)")
print(f"  spark.sql.extensions   = {exts}")
print(f"  spark.shuffle.manager  = {conf.get('spark.shuffle.manager', '(not set: sort)')}")
print(f"  spark.memory.offHeap   = {conf.get('spark.memory.offHeap.enabled', 'false')}, "
      f"{conf.get('spark.memory.offHeap.size', '0')}")
rules = [short(r.getClass().getName()) for r in seq(state.columnarRules())]
print(f"  columnar rules         = {', '.join(rules) or '(none)'}")
prep = [short(r.getClass().getName()) for r in seq(jss.extensions().buildQueryStagePrepRules(jss))]
print(f"  query stage prep rules = {', '.join(prep) or '(none)'}")
rapids = sorted((k, v) for k, v in conf.getAll() if k.startswith("spark.rapids.memory"))
for k, v in rapids:
    print(f"  {k:<22} = {v}")
