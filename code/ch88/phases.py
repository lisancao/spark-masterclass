"""Chapter 88, Step 1-2: the phases of a query, as text and as JVM objects.

Classic only (df._jdf reaches the JVM Dataset through Py4J).
Run from code/:  uv run python ch88/phases.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session
from logs import access_parquet, _area

spark = session("ch88-phases")
df = access_parquet(spark)
area = F.udf(_area, "string")

print(f"rows {df.count()}, status 200: {df.where('status = 200').count()}, "
      f"api among them: {df.where('status = 200').where(area('path') == 'api').count()}\n")

q = df.filter((F.col("status") == 200) & (area("path") == "api"))
q.explain("extended")

qe = q._jdf.queryExecution()
print("logical      ", qe.logical().getClass().getName())
print("analyzed     ", qe.analyzed().getClass().getName())
print("optimizedPlan", qe.optimizedPlan().getClass().getName())
print("sparkPlan    ", qe.sparkPlan().getClass().getName())
print("executedPlan ", qe.executedPlan().getClass().getName())


def walk(plan, depth=0):
    """Every node of a physical plan, top-down, descending into AdaptiveSparkPlan by hand."""
    yield depth, plan
    if plan.nodeName() == "AdaptiveSparkPlan":
        yield from walk(plan.executedPlan(), depth + 1)
        return
    children = plan.children()
    for i in range(children.length()):
        yield from walk(children.apply(i), depth + 1)


print("\nThe executed plan as a tree of JVM objects:")
for depth, node in walk(qe.executedPlan()):
    print(f"  {'  ' * depth}{node.nodeName()!r}")

phases = qe.tracker().phases()
it = phases.iterator()
print("\nqe.tracker().phases():")
while it.hasNext():
    kv = it.next()
    s = kv._2()
    print(f"  {kv._1():<13}{s.endTimeMs() - s.startTimeMs():>4} ms")
