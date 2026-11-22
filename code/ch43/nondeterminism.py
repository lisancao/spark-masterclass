"""Chapter 43, Step 3: what Catalyst does with a nondeterministic expression, and what
4.3's UDF transpilation does to a UDF with a side effect.

Run from code/:  uv run python ch43/nondeterminism.py
"""
import glob
import os
import tempfile

from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from chaos import fresh, first_line

spark = fresh("local[4]")
calls = spark.sparkContext.accumulator(0)


def plus_one(x):
    calls.add(1)
    return x + 1


det = F.udf(plus_one, "long")
nondet = F.udf(plus_one, "long").asNondeterministic()
base = spark.range(0, 1000, numPartitions=4)
for name, u in [("deterministic", det), ("asNondeterministic", nondet)]:
    calls.value = 0
    q = base.withColumn("y", u("id")).where("id < 10")
    q.collect()
    print(f"{name}: {calls.value} UDF calls for 10 rows")
    print(q._jdf.queryExecution().optimizedPlan().toString())

# A nondeterministic expression where Spark cannot allow one.
a = spark.range(5)
b = spark.range(5).withColumnRenamed("id", "j")
try:
    a.join(b, (a.id == b.j) & (F.rand(1) > 0.5)).collect()
except Exception as e:
    print(e.getCondition(), "|", first_line(e, 200)[:150])

# The deterministic flag is only a promise. A UDF that also writes a file "looks" pure to Catalyst.
# 4.3 can transpile simple Python UDFs into Catalyst expressions. What happens to the side effect?
marks = tempfile.mkdtemp(prefix="ch43-marks-")


def plus_one_and_mark(x):
    open(f"{marks}/{x}", "w").close()            # a side effect: one file per call
    return x + 1


def add_one(x):
    return x + 1


for flag in ("false", "true"):
    spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", flag)
    for fn, arrow in ((add_one, None), (add_one, False), (plus_one_and_mark, False)):
        for f in glob.glob(f"{marks}/*"):
            os.remove(f)
        u = F.udf(fn, LongType(), useArrow=arrow)    # useArrow=None follows the session default
        q = spark.range(0, 100, numPartitions=2).select(u("id").alias("y"))
        rows = q.collect()
        python_in_plan = "EvalPython" in q._jdf.queryExecution().executedPlan().toString()
        print(f"transpile={flag:5} {fn.__name__:17} useArrow={arrow!s:5} evalType={u.evalType}  "
              f"Python in plan: {python_in_plan!s:5}  files: {len(glob.glob(f'{marks}/*'))}")
spark.conf.unset("spark.sql.experimental.optimizer.transpilePyUDFs")

# A UDF pickles its function and the globals it reads once, the first time it builds a column.
CFG = {"x": 1}
u = F.udf(lambda v: CFG["x"], LongType())
df1 = spark.range(1).select(u("id").alias("x"))
CFG["x"] = 2
df2 = spark.range(1).select(u("id").alias("x"))
df3 = spark.range(1).select(F.udf(lambda v: CFG["x"], LongType())("id").alias("x"))
print("CFG changed from 1 to 2 between df1 and df2:",
      "df1 =", df1.first().x, " df2 =", df2.first().x, " a new udf object =", df3.first().x)
spark.stop()
