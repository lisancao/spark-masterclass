"""Chapter 59, Steps 10-11: why Python nodes split whole-stage codegen, and the columnar path into Arrow UDFs.

Run from code/:  uv run python ch59/codegen.py
"""
import re, sys, tempfile, threading, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan
from logs import access_parquet, _area

spark = session("ch59-codegen")
df = access_parquet(spark)
area = F.udf(_area, "string")

q = df.filter(F.col("status") == 200).withColumn("a", area("path"))
code = plan(q, "codegen").splitlines()
seen = ['']
keep = False
for l in code:
    if l.startswith(("Found", "== Subtree")):
        keep = True
    if l.startswith("Generated code:"):
        keep = False
        print("Generated code:")
    if keep and l.strip():
        print(re.sub(r"Batched: true, .*", "...", l))
    m = re.match(r"/\* (\d+) \*/", l)
    if m and 38 <= int(m.group(1)) <= 47 and "Subtree 1" in seen[-1]:
        print(l)
    if l.startswith("== Subtree"):
        seen.append(l)
print()

# The columnar path: an Arrow UDF directly over a vectorized Parquet scan.
print("spark.sql.execution.arrow.pythonUDF.columnarInput.enabled =",
      spark.conf.get("spark.sql.execution.arrow.pythonUDF.columnarInput.enabled"))
p = tempfile.mkdtemp(prefix="ch59-") + "/s"
spark.range(3).selectExpr("cast(id as string) s", "cast(id as int) n").write.parquet(p)
src = spark.read.parquet(p)
bang = F.udf(lambda s: s + "!", "string")
shapes = {
    "@udf (101) on a column":              src.select(bang("s")),
    "@udf, useArrow=False (100)":          src.select(F.udf(lambda s: s + "!", "string", useArrow=False)("s")),
    "pandas_udf (200) on a column":        src.select(F.pandas_udf(lambda s: s + "!", "string")("s")),
    "@udf on concat(s, 'x')":              src.select(bang(F.concat("s", F.lit("x")))),
    "@udf on n + 0":                       src.select(F.udf(lambda n: n + 1, "int")(F.col("n") + 0)),
    "a Filter between scan and UDF":       src.filter("n >= 0").select(bang("s")),
}
for name, q in shapes.items():
    text = plan(q, "simple")
    columnar = "ColumnarToRow" not in text
    result = {}
    t = threading.Thread(target=lambda: result.update(rows=q.collect()), daemon=True)
    start = time.perf_counter(); t.start(); t.join(60)
    outcome = f"{len(result['rows'])} rows in {time.perf_counter() - start:.2f} s" if "rows" in result \
        else "no result after 60 s"
    if name.startswith("@udf on concat"):
        print("\n".join("    " + l for l in text.splitlines()[1:4]))
    print(f"  {name:<32} columnar input: {str(columnar):<5}  {outcome}")
