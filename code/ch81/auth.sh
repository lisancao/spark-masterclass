#!/usr/bin/env bash
# Chapter 81, Steps 1 and 2: spark.authenticate and AES-based RPC encryption on a standalone cluster.
# The cluster is Chapter 80's (ch80/standalone.sh), restarted with a shared secret in spark-defaults.conf.
# A proxy between the driver and the master records what the driver sends.
#   ch81/auth.sh        (from code/)
set -uo pipefail
cd "$(dirname "$0")"
source ../ch80/env.sh
W=$BOOK_DATA/work/ch81; mkdir -p "$W"
SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
cat > "$W/secure.conf" <<CONF
spark.authenticate                      true
spark.authenticate.secret               $SECRET
spark.network.crypto.enabled            true
spark.network.crypto.cipher             AES/GCM/NoPadding
spark.network.crypto.authEngineVersion  2
CONF
../ch80/standalone.sh down
CH80_EXTRA_DEFAULTS=$W/secure.conf ../ch80/standalone.sh up
echo "cluster: master and workers read spark.authenticate=true and the secret from spark-defaults.conf"
echo "secret in a daemon command line? $(ps -o args= -p "$(cat "$SPARK_PID_DIR"/*Master-1.pid)" | grep -c "$SECRET")"

python3 proxy.py 7161 7151 "$W/wire.bin" & PROXY=$!
sleep 1
grep -v -E "authenticate|crypto" "$SPARK_CONF_DIR/spark-defaults.conf" > "$W/plain.conf"
run() {  # label, then spark-submit options
  local label=$1; shift
  : > "$W/wire.bin"
  local ML; ML=$(command ls "$SPARK_LOG_DIR"/*Master-1-*.out | head -1); local before; before=$(wc -l < "$ML")
  printf '%-34s ' "$label"
  timeout 60 "$SPARK_HOME/bin/spark-submit" --master spark://127.0.0.1:7161 "$@" probe.py "ch81-$label" \
    > "$W/run.out" 2> "$W/run.err"
  local rc=$?
  if grep -q "ok," "$W/run.out"; then cat "$W/run.out"
  else echo "FAILED (driver gave up after 60 s, exit $rc)"
    printf '%-34s master log: %s\n' "" "$(tail -n +$((before + 1)) "$ML" | grep -m1 -o -E '(Exception|Error): .*' | cut -c1-100)"; fi
  printf '%-34s %s bytes to the master, app name visible: %s, "spark." strings: %s\n' "" \
    "$(stat -c %s "$W/wire.bin")" "$(grep -a -c "ch81-$label" "$W/wire.bin")" "$(grep -a -o "spark\.[a-z.]*" "$W/wire.bin" | wc -l)"
}
run no-secret          --properties-file "$W/plain.conf"
run wrong-secret       --properties-file "$W/plain.conf" --conf spark.authenticate=true --conf spark.authenticate.secret=wrong
run secret-no-crypto   --conf spark.network.crypto.enabled=false
run secret-aes-gcm
echo "--- what the eavesdropper read in the secret-no-crypto run (printable strings, first lines)"
run secret-no-crypto --conf spark.network.crypto.enabled=false >/dev/null
strings -n 8 "$W/wire.bin" | grep -E "ch81|spark\.[a-z]" | head -8 | cut -c1-90
echo "(the shared secret itself in those bytes: $(grep -a -c "$SECRET" "$W/wire.bin") times)"
echo "--- the same bytes with AES/GCM"
run secret-aes-gcm >/dev/null
strings -n 8 "$W/wire.bin" | head -3 | cut -c1-90; echo "(printable strings of 8+ chars: $(strings -n 8 "$W/wire.bin" | wc -l))"
kill $PROXY
