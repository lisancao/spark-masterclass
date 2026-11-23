#!/usr/bin/env bash
# Chapter 42: GC time with a cache of objects or a serialized cache, two collectors, and the GC log.
# Timings: run under the bench lock:  flock ~/spark-masterclass/data/.bench.lock bash ch42/gc.sh
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
shell=$here/../.venv/lib/python3.12/site-packages/pyspark/bin/spark-shell
logs=${GC_LOGS:-/tmp/masterclass-ch42-gc}; rm -rf "$logs"; mkdir -p "$logs"
KRYO="--conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.classesToRegister=book.Trip"
run() {
  local label=$1 level=$2 name=$3 jopts=$4; shift 4
  GC_LABEL=$label GC_LEVEL=$level "$shell" --master 'local[*]' --driver-memory 2g --conf spark.ui.port=${SPARK_UI_PORT:-4040} \
    --conf spark.ui.showConsoleProgress=false --jars "$here/trip.jar" \
    --driver-java-options "$jopts -Xlog:gc:file=$logs/$name.log" "$@" -i "$here/gc.scala" 2>/dev/null | grep -E "^(G1|Parallel)"
  local pauses young full total
  pauses=$(grep -c 'Pause' "$logs/$name.log")
  young=$(grep -c 'Pause Young' "$logs/$name.log")
  full=$(grep -c 'Pause Full' "$logs/$name.log")
  total=$(grep -o 'Pause[^)]*) [0-9.]*M->[0-9.]*M([0-9.]*M) [0-9.]*ms' "$logs/$name.log" | awk '{s+=$NF+0} END {printf "%.0f", s}')
  printf '%-34s GC log: %s pauses (%s young, %s full), %s ms in pauses\n' "" "$pauses" "$young" "$full" "$total"
}
run "G1, MEMORY_ONLY (objects)" MEMORY_ONLY g1-objects ""
run "G1, MEMORY_ONLY_SER (Kryo)" MEMORY_ONLY_SER g1-ser "" $KRYO
run "Parallel, MEMORY_ONLY (objects)" MEMORY_ONLY par-objects "-XX:+UseParallelGC"
run "Parallel, MEMORY_ONLY_SER (Kryo)" MEMORY_ONLY_SER par-ser "-XX:+UseParallelGC" $KRYO
echo
echo "the first lines of one GC log (-Xlog:gc):"
head -6 "$logs/g1-objects.log" | sed 's/^/  /'
echo
echo "the flags in the tuning guide, on Java $(java -version 2>&1 | head -1 | cut -d'"' -f2):"
java -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -version 2>&1 | head -3 | sed 's/^/  /'
