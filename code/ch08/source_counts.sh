#!/usr/bin/env bash
# Chapter 8: find configs in the Spark source at tag v4.3.0, and count them.
# SQL configs are declared with buildConf / buildStaticConf, core configs with ConfigBuilder.
# Needs a clone of apache/spark with the v4.3.0 tag (SPARK_SRC, default ~/repos/spark).
# Run from code/:  ch08/source_counts.sh > ch08/output/source_counts.txt
set -euo pipefail
SRC=${SPARK_SRC:-$HOME/repos/spark}
T=v4.3.0
g() { git -C "$SRC" show "$T:$1"; }
SQLCONF=sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala
STATIC=sql/catalyst/src/main/scala/org/apache/spark/sql/internal/StaticSQLConf.scala

echo "== 1. one config, as the source declares it ($T, $SQLCONF)"
g $SQLCONF | grep -n -A8 'val SHUFFLE_PARTITIONS = buildConf' | sed 's/^/  /'

echo
echo "== 2. how many configs each file declares"
# a declaration is buildConf(" / buildStaticConf(" / ConfigBuilder("
count() { g "$1" | grep -c -E "$2" || true; }
internal() {  # declarations followed by .internal() within the next 3 lines
  g "$1" | awk -v pat="$2" '$0 ~ pat {n=4; d++} n>0 && /\.internal\(\)/ {i++; n=0} n>0 {n--} END {print i+0}'
}
printf "  %-62s %6s %9s\n" "file" "total" "internal"
printf "  %-62s %6s %9s\n" "$SQLCONF" "$(count $SQLCONF 'buildConf\(')" "$(internal $SQLCONF 'buildConf\(')"
printf "  %-62s %6s %9s\n" "$STATIC" "$(count $STATIC 'buildStaticConf\(')" "$(internal $STATIC 'buildStaticConf\(')"
core_total=0; core_int=0
for f in $(git -C "$SRC" ls-tree -r --name-only $T core/src/main/scala/org/apache/spark/internal/config/); do
  core_total=$((core_total + $(count "$f" 'ConfigBuilder\("')))
  core_int=$((core_int + $(internal "$f" 'ConfigBuilder\("')))
done
printf "  %-62s %6s %9s\n" "core/src/main/scala/org/apache/spark/internal/config/*.scala" "$core_total" "$core_int"

echo
echo "== 3. the files that declare configs, outside those three places (top 10)"
git -C "$SRC" grep -c -E 'buildConf\("|buildStaticConf\("|ConfigBuilder\("' $T -- '*.scala' \
  | sed "s#^$T:##" | grep -v -E 'SQLConf.scala|StaticSQLConf.scala|/internal/config/|/test/' \
  | sort -t: -k2 -n -r | head -10 | awk -F: '{printf "  %4d  %s\n", $2, $1}'
