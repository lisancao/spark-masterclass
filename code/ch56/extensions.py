"""Chapter 56: a native function, an optimizer rule and a check rule, from one extension jar.

Build the jar first:   ch56/build.sh
Run from code/:        uv run python ch56/extensions.py
"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import ROOT, session  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

JAR = Path(__file__).resolve().parent / "jext" / "target" / "jext-0.1.0.jar"
IMAGES = ROOT / "code" / "data" / "messy" / "images"
PNG_SIG = b"\x89PNG\r\n\x1a\n"

spark = session("ch56-extensions", spark_jars=str(JAR), spark_sql_extensions="jext.Extensions")


def physical(df):
    return df._jdf.queryExecution().executedPlan().toString().replace(str(ROOT), "<repo>")


def head(t):
    print(f"\n== {t}")


def error(e):
    return f"{type(e).__name__}: {str(e).splitlines()[0][:150]}"


head("1. The function is registered")
spark.sql("DESCRIBE FUNCTION EXTENDED is_png").show(truncate=False)

head("2. is_png on literals")
spark.sql("""SELECT is_png(X'89504E470D0A1A0A') AS sig, is_png(X'89504E470D0A1A0A0000') AS longer,
                    is_png(X'89504E470D0A1A') AS short, is_png(X'') AS empty,
                    is_png(X'FF504E470D0A1A0A') AS wrong, is_png(CAST(NULL AS BINARY)) AS null_in,
                    is_png(NULL) AS untyped_null""").show()

head("3. Errors at analysis")
for q in ["SELECT is_png('not binary')", "SELECT is_png(X'00', X'01')"]:
    try:
        spark.sql(q).collect()
    except Exception as e:
        print(q, "->", error(e))

head("4. On real files, from Python")
flat = spark.read.format("binaryFile").load(str(IMAGES))
print("binaryFile without recursiveFileLookup:", flat.count(), "rows")
imgs = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(str(IMAGES))
(imgs.select(F.substring_index("path", "/", -1).alias("file"),
             F.call_function("is_png", "content").alias("call_function"),
             F.expr("is_png(content)").alias("expr"))
 .orderBy("file").show(truncate=False))

head("5. Code generation: CodegenFallback vs StaticInvoke")
for fn in ("is_png", "is_png_fast"):
    print(physical(imgs.select(F.call_function(fn, "content"))).splitlines()[0][:110])

head("6. The expression can run on the driver")
df = spark.sql("SELECT is_png(content) AS p FROM VALUES (X'00') AS t(content)")
print("optimized:", df._jdf.queryExecution().optimizedPlan().toString().strip())
print("result:", df.collect())


@F.udf("boolean")
def is_png(b):
    return b is not None and bytes(b[:8]) == PNG_SIG


@F.udf("boolean")
def looks_like_png(b):          # the same body under another name: the rule leaves it alone
    return b is not None and bytes(b[:8]) == PNG_SIG


head("7. The rule replaces the Python UDF is_png")
print("looks_like_png (not rewritten):")
print(physical(imgs.filter(looks_like_png("content")).select("path")))
print("is_png (rewritten by jext.PngRule):")
print(physical(imgs.filter(is_png("content")).select("path")))

head("8. NULL: the rewrite keeps the UDF's answer")
files = imgs.select("content").unionByName(spark.createDataFrame([(None,)], "content binary"))
for name, fn in (("Python looks_like_png", looks_like_png("content")),
                 ("rewritten is_png", is_png("content")),
                 ("native is_png alone", F.call_function("is_png", "content"))):
    vals = sorted((r[0] for r in files.select(fn).collect()), key=repr)
    n_not = files.filter(~fn).count()
    print(f"{name:22} values {vals} | rows where NOT holds: {n_not}")
print("rewrites so far:", spark._jvm.jext.PngRule.rewrites())

head("9. UDFs the rule must leave alone")
keep = {"another name": looks_like_png("content"),
        "a string result": F.udf(lambda b: "yes", "string")("content"),
        "nondeterministic": is_png.asNondeterministic()("content")}
for why, col in keep.items():
    nodes = [l.strip(" +-:").split(" [")[0] for l in physical(imgs.select(col)).splitlines() if "EvalPython" in l]
    print(f"{why:18} -> still in Python: {nodes}")

head("10. The check rule")
try:
    grouped = imgs.groupBy("content").count()        # no action: the error comes from analysis
except Exception as e:
    print("groupBy('content').count() ->", error(e))
n = imgs.groupBy(F.sha2("content", 256)).count().count()
print("groupBy(sha2(content, 256)) is allowed:", n, "groups")
spark.stop()
