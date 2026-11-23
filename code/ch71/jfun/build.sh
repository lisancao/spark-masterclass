#!/usr/bin/env bash
# Build jfun.jar (a Hive GenericUDF, a Java UDF1, an Aggregator and a legacy UDAF) with javac
# against the jars of the pyspark install (they include hive-exec 2.3.10).
#   ch71/jfun/build.sh   ->  ch71/jfun/target/jfun.jar
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
jars=$(cd "$here/../.." && uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")/jars
rm -rf "$here/target" && mkdir -p "$here/target/classes"
javac --release 17 -Xlint:none -d "$here/target/classes" -cp "$jars/*" "$here"/src/jfun/*.java
jar cf "$here/target/jfun.jar" -C "$here/target/classes" .
echo "built target/jfun.jar ($(javac -version 2>&1)) against $(ls "$jars" | grep -c jar) jars"
