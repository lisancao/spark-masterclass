"""Chapter 53: the five traps of a Catalyst Expression written in Java, each one built and run.

Each variant changes IsPng.java in one place, compiles the jar with javac against pyspark's own
jars, and runs one query in a fresh process (spark.jars is fixed when a session starts).
Run from code/:  uv run python ch53/traps.py
"""
import glob, os, re, shutil, subprocess, sys, tempfile, textwrap
from pathlib import Path
import pyspark

SRC = Path(__file__).resolve().parent / "jext" / "src" / "main" / "java"
JARS = os.pathsep.join(glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "*.jar")))
IMAGES = Path(__file__).resolve().parents[1] / "data" / "messy" / "images"

ORIG = (SRC / "jext" / "IsPng.java").read_text()
VARIANTS = {
    "1. extend UnaryExpression": (
        lambda s: s.replace("public class IsPng extends Expression",
                            "public class IsPng extends org.apache.spark.sql.catalyst.expressions.UnaryExpression"),
        None),
    "2. not Serializable": (
        lambda s: s.replace("implements CodegenFallback, Serializable", "implements CodegenFallback"),
        f"spark.read.format('binaryFile').option('recursiveFileLookup','true').load('{IMAGES}')"
        ".selectExpr('is_png(content)').collect()"),
    "3. eval returns null, nullable follows the child": (
        lambda s: s.replace("return v == null ? null : check((byte[]) v);", "return null;"),
        "spark.sql(\"SELECT is_png(X'89504E470D0A1A0A') AS p\").collect()"),
    "4. no checkInputDataTypes": (
        lambda s: re.sub(r"  @Override public TypeCheckResult checkInputDataTypes\(\) \{.*?\n  \}\n", "", s, flags=re.S),
        "spark.sql(\"SELECT is_png(s) FROM VALUES ('not binary') AS t(s)\").collect()"),
    "5. BINARY only, no NullType": (
        lambda s: s.replace("t instanceof BinaryType || t instanceof NullType", "t instanceof BinaryType"),
        "spark.sql('SELECT is_png(NULL)').collect()"),
}

RUN = textwrap.dedent("""
    import sys
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master('local[2]').config('spark.jars', sys.argv[1])
             .config('spark.sql.extensions', 'jext.Extensions').config('spark.ui.enabled', 'false')
             .getOrCreate())
    spark.sparkContext.setLogLevel('OFF')
    try:
        {query}
        print('no error')
    except Exception as e:
        print(type(e).__name__ + ': ' + str(e))
""")

KEYS = ("Task not serializable", "NotSerializableException", "NullPointerException", "GeneratedClass",
        "ClassCastException", "DATATYPE_MISMATCH", "error:")

for title, (change, query) in VARIANTS.items():
    work = Path(tempfile.mkdtemp(prefix="jext-trap-"))
    shutil.copytree(SRC, work / "src")
    f = work / "src" / "jext" / "IsPng.java"
    f.write_text(change(ORIG))
    assert f.read_text() != ORIG, title
    print(f"\n== {title}")
    r = subprocess.run(["javac", "--release", "17", "-d", str(work / "classes"), "-cp", JARS,
                        *map(str, sorted((work / "src").rglob("*.java")))], capture_output=True, text=True)
    if r.returncode:
        for line in r.stderr.replace(str(work), "").splitlines():
            if "error:" in line or line.strip().startswith(("withNewChildren", "Expression")):
                print("javac:", line.strip()[:160])
        continue
    jar = work / "jext.jar"
    subprocess.run(["jar", "cf", str(jar), "-C", str(work / "classes"), "."], check=True)
    out = subprocess.run([sys.executable, "-c", RUN.format(query=query), str(jar)],
                         capture_output=True, text=True).stdout
    seen = set()
    for line in out.splitlines():
        line = line.strip()
        for k in KEYS:
            if k in line and k not in seen:
                seen.add(k)
                print(line[line.index(k):][:150])
    if not seen:
        print(out.strip()[:300])
