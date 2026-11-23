#!/usr/bin/env bash
# Chapter 82: compare the hadoop-provided build with the official 4.3.0 tarball, then run it.
#   ch82/inspect.sh <path to spark-4.3.0-bin-book.tgz>        (from code/)
set -uo pipefail
cd "$(dirname "$0")"
export BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data}
OFFICIAL=$BOOK_DATA/tarball/spark-4.3.0-bin-hadoop3
TGZ=$1
X=$BOOK_DATA/work/ch82; rm -rf "$X"; mkdir -p "$X"; tar xzf "$TGZ" -C "$X"
OURS=$(echo "$X"/spark-4.3.0-bin-book)
row() { printf '%-26s %-34s %s\n' "$1" "$2" "$3"; }
row "" "official (-bin-hadoop3)" "this build (-bin-book)"
row "tgz size" "$(du -h "$BOOK_DATA/tarball/spark-4.3.0-bin-hadoop3.tgz" | cut -f1)" "$(du -h "$TGZ" | cut -f1)"
row "jars/" "$(command ls "$OFFICIAL/jars" | wc -l) jars" "$(command ls "$OURS/jars" | wc -l) jars"
row "hadoop-* jars" "$(command ls "$OFFICIAL/jars" | grep -c '^hadoop-')" "$(command ls "$OURS/jars" | grep -c '^hadoop-')"
row "RELEASE, first line" "$(head -1 "$OFFICIAL/RELEASE")" "$(head -1 "$OURS/RELEASE")"
row "RELEASE, build flags" "$(sed -n 2p "$OFFICIAL/RELEASE" | cut -c1-34)" "$(sed -n 2p "$OURS/RELEASE")"
MISSING=$(comm -23 <(command ls "$OFFICIAL/jars") <(command ls "$OURS/jars"))
echo "--- jars in the official tarball and not in this build: $(echo "$MISSING" | wc -l)"
echo "  Spark modules (no -Phive -Phive-thriftserver -Pyarn in this build):"
echo "$MISSING" | grep '^spark-' | sed 's/^/    /'
echo "  Hadoop and the libraries that a Hadoop install provides (hadoop-provided), for example:"
echo "$MISSING" | grep -E '^(hadoop-|slf4j|zookeeper-3|curator-client|guava|commons-cli)' | sed 's/^/    /'
echo "  Hive and its dependencies: $(echo "$MISSING" | grep -c -E '^(hive-|datanucleus|derby|javax.jdo|jdo-|libthrift|libfb303|antlr-runtime|ST4|HikariCP|jpam|jta|transaction-api|super-csv|javolution|stax-api|json-1|commons-dbcp|commons-pool|commons-lang-2|jline|joda|httpclient|httpcore)') jars"
echo "--- jars in this build and not in the official tarball"
comm -13 <(command ls "$OFFICIAL/jars") <(command ls "$OURS/jars") | sed 's/^/  /'
echo "--- run it without Hadoop on the class path"
echo 'spark.range(10).count()' > "$X/q.py"
"$OURS/bin/spark-submit" --master "local[1]" "$X/q.py" 2>&1 | grep -m2 -E "NoClassDefFoundError|ClassNotFoundException|Exception in thread" | cut -c1-110
echo "--- run it with SPARK_DIST_CLASSPATH set to a Hadoop client and slf4j (here, jars of the official tarball)"
printf 'from pyspark.sql import SparkSession\ns = SparkSession.builder.getOrCreate()\nprint("count:", s.range(10).count(), " version:", s.version)\n' > "$X/q.py"
SPARK_DIST_CLASSPATH="$(command ls "$OFFICIAL"/jars/hadoop-*.jar "$OFFICIAL"/jars/slf4j-api-*.jar | tr '\n' ':')" PYSPARK_PYTHON=python3 \
  "$OURS/bin/spark-submit" --master "local[1]" "$X/q.py" 2>/dev/null
