#!/usr/bin/env bash
# Compile the two Hive UDFs against the Hive and Hadoop jars that pyspark 4.3.0 ships, and pack a jar.
#   code/ch17/hive_udf/build.sh   ->  code/ch17/hive_udf/book-hive-udf.jar
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
jars=$(cd "$here/../.." && .venv/bin/python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__) + '/jars')")
out=$(mktemp -d)
javac --release 17 -cp "$jars/*" -d "$out" "$here"/src/book/udf/*.java
jar --create --file "$here/book-hive-udf.jar" -C "$out" .
rm -rf "$out"
jar --list --file "$here/book-hive-udf.jar" | grep class
