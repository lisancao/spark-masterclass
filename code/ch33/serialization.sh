#!/usr/bin/env bash
# Chapter 33: the same Scala job with Java serialization, Kryo, Kryo with registration, and Kryo with
# registration required but missing. Run from code/:  bash ch33/serialization.sh
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
shell=$here/../.venv/lib/python3.12/site-packages/pyspark/bin/spark-shell
run() {
  local label=$1; shift
  SER_LABEL=$label "$shell" --master 'local[*]' --driver-memory 4g --conf spark.ui.port=${SPARK_UI_PORT:-4040} \
    --conf spark.ui.showConsoleProgress=false "$@" -i "$here/serialization.scala" 2>/dev/null | grep -E '^(java|kryo)'
}
KRYO=org.apache.spark.serializer.KryoSerializer
run "java (default)"
run "kryo, not registered" --conf spark.serializer=$KRYO
run "kryo, registered" --conf spark.serializer=$KRYO --conf spark.kryo.classesToRegister='$line3.$read$$iw$Trip'
run "kryo, required, none" --conf spark.serializer=$KRYO --conf spark.kryo.registrationRequired=true
