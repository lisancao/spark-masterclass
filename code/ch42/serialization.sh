#!/usr/bin/env bash
# Chapter 42: the same Scala job with Java serialization, Kryo, Kryo with registration, and Kryo with
# registration required but missing. Timings: run under the bench lock:
#   flock ~/spark-masterclass/data/.bench.lock bash ch42/serialization.sh
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
shell=$here/../.venv/lib/python3.12/site-packages/pyspark/bin/spark-shell
run() {
  local label=$1; shift
  SER_LABEL=$label "$shell" --master 'local[*]' --driver-memory 4g --conf spark.ui.port=${SPARK_UI_PORT:-4040} \
    --conf spark.ui.showConsoleProgress=false --jars "$here/trip.jar" "$@" -i "$here/serialization.scala" 2>/dev/null | grep -E "^(java|kryo)"
}
KRYO=org.apache.spark.serializer.KryoSerializer
run "java (default)"
run "kryo, not registered" --conf spark.serializer=$KRYO
run "kryo, registered" --conf spark.serializer=$KRYO --conf spark.kryo.classesToRegister=book.Trip
run "kryo, required, none" --conf spark.serializer=$KRYO --conf spark.kryo.registrationRequired=true
run "kryo, required, registered" --conf spark.serializer=$KRYO --conf spark.kryo.registrationRequired=true \
  --conf spark.kryo.classesToRegister=book.Trip
