#!/usr/bin/env bash
# Chapter 77, Step 8: test the fix without building Spark.
# Takes Analyzer.scala and ViewResolution.scala at v4.3.0, applies fix/narrow-fix-v4.3.0.diff, compiles the two
# files against the jars inside the pyspark 4.3.0 wheel with the Scala compiler that ships there, and runs
# the reproduction and the boundary table again with the new classes first on the driver class path.
#   Run from code/:  ch77/test_fix.sh        (SPARK_SRC=<a clone of apache/spark> reads the files from it)
set -euo pipefail
cd "$(dirname "$0")/.."
work=$(mktemp -d -t ch77-fix-XXXXXX)
jars=$(uv run python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
files=(sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala
       sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ViewResolution.scala)
for f in "${files[@]}"; do
  mkdir -p "$work/src/$(dirname "$f")"
  if [[ -n ${SPARK_SRC:-} ]]; then
    git -C "$SPARK_SRC" show "v4.3.0:$f" > "$work/src/$f" 2>/dev/null
  else
    curl -fsSL "https://raw.githubusercontent.com/apache/spark/v4.3.0/$f" -o "$work/src/$f"
  fi
done
(cd "$work/src" && patch -s -p1 < "$OLDPWD/ch77/fix/narrow-fix-v4.3.0.diff")
echo "patched: ${files[*]##*/}"
cp=$(ls "$jars"/*.jar | tr '\n' ':')
scala=$(ls "$jars"/scala-{compiler,library,reflect}-*.jar | tr '\n' ':')
mkdir -p "$work/classes"
start=$(date +%s)
java -Xss8m -Xmx3g -cp "$scala" scala.tools.nsc.Main -nowarn -release 17 -classpath "$cp" \
  -d "$work/classes" "${files[@]/#/$work/src/}"
echo "compiled with $(ls "$jars" | grep scala-compiler) in $(( $(date +%s) - start )) s:" \
     "$(find "$work/classes" -name '*.class' | wc -l) class files"
echo
PATCHED_CLASSES="$work/classes" uv run python ch77/repro.py 2>/dev/null
echo
PATCHED_CLASSES="$work/classes" uv run python ch77/boundaries.py 2>/dev/null | sed -n '1,10p'
