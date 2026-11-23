#!/usr/bin/env bash
# Chapter 71, Steps 3 and 4: TLS for the UI and for RPC, then UI ACLs behind a servlet filter.
#   ch71/ssl_acl.sh        (from code/)
set -uo pipefail
cd "$(dirname "$0")"
export BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data}
SUBMIT="$BOOK_DATA/tarball/spark-4.3.0-bin-hadoop3/bin/spark-submit"
export PYSPARK_PYTHON=$(cd .. && pwd)/.venv/bin/python SPARK_LOCAL_IP=127.0.0.1
W=$BOOK_DATA/work/ch71; mkdir -p "$W"; rm -f "$W"/*.p12 "$W"/*.pem
PW=book-$RANDOM$RANDOM
# 1. a self-signed key pair for localhost, and a trust store with its certificate
keytool -genkeypair -alias spark -keyalg RSA -keysize 2048 -validity 2 -dname "CN=localhost" \
  -ext "SAN=dns:localhost,ip:127.0.0.1" -storetype PKCS12 -keystore "$W/keystore.p12" \
  -storepass "$PW" -keypass "$PW" 2>/dev/null
keytool -exportcert -rfc -alias spark -keystore "$W/keystore.p12" -storepass "$PW" > "$W/spark.pem" 2>/dev/null
keytool -importcert -noprompt -alias spark -file "$W/spark.pem" -storetype PKCS12 \
  -keystore "$W/truststore.p12" -storepass "$PW" 2>/dev/null
echo "key store and trust store: $(command ls "$W" | grep -E 'p12|pem' | tr '\n' ' ')"

cat > "$W/ssl.conf" <<CONF
spark.ssl.enabled                true
spark.ssl.keyStore               $W/keystore.p12
spark.ssl.keyStoreType           PKCS12
spark.ssl.keyStorePassword       $PW
spark.ssl.keyPassword            $PW
spark.ssl.trustStore             $W/truststore.p12
spark.ssl.trustStoreType         PKCS12
spark.ssl.trustStorePassword     $PW
CONF
wait_ui() { for _ in $(seq 60); do curl -s -o /dev/null "$1" -k && return; sleep 1; done; }

echo "--- UI with spark.ssl.enabled=true (the ui namespace inherits it)"
$SUBMIT --master "local[2]" --properties-file "$W/ssl.conf" --conf spark.ui.port=4151 \
  probe.py ch71-ssl-ui --hold 12 > "$W/ssl.out" 2> "$W/ssl.err" & APP=$!
wait_ui https://localhost:4551/
curl -s -o /dev/null -w "http://localhost:4151/jobs/      -> %{http_code} %{redirect_url}\n" http://localhost:4151/jobs/
curl -s -o /dev/null -w "https://localhost:4551/jobs/     -> %{http_code} (trusted with spark.pem)\n" --cacert "$W/spark.pem" https://localhost:4551/jobs/
curl -s -o /dev/null -w "https, no trust store            -> %{http_code} curl exit %{exitcode}\n" https://localhost:4551/jobs/
echo | openssl s_client -connect localhost:4551 2>/dev/null | grep -E "^(subject|issuer)=|Cipher is" | sed 's/^ *//'
grep -o "SecurityManager: SecurityManager: .*" "$W/ssl.err" | head -1 | grep -o "RPC SSL [a-z]*"
wait $APP

echo "--- RPC with spark.ssl.rpc.enabled=true (not inherited, must be set) on local-cluster"
$SUBMIT --master "local-cluster[2,1,1024]" --properties-file "$W/ssl.conf" --conf spark.ui.port=4151 \
  --conf spark.ssl.rpc.enabled=true --conf spark.ssl.ui.enabled=false \
  probe.py ch71-ssl-rpc > "$W/rpc.out" 2> "$W/rpc.err"
cat "$W/rpc.out"
grep -o "SecurityManager: SecurityManager: .*" "$W/rpc.err" | head -1 | grep -o "RPC SSL [a-z]*"
grep -o -E "Successfully created connection to [^ ]+ after [0-9]+ ms \([0-9]+ ms spent in bootstraps\)" "$W/rpc.err" | head -2

echo "--- UI ACLs: spark.acls.enable=true, view: alice, admin: ops, a header filter for the user"
mkdir -p "$W/filter"
javac -d "$W/filter" -cp "$(echo ../.venv/lib/python3*/site-packages/pyspark/jars/jakarta.servlet-api-*.jar)" filter/book/HeaderUserFilter.java
jar cf "$W/book-filter.jar" -C "$W/filter" .
$SUBMIT --master "local[2]" --jars "$W/book-filter.jar" --conf spark.ui.port=4151 \
  --conf spark.ui.filters=book.HeaderUserFilter \
  --conf spark.acls.enable=true --conf spark.ui.view.acls=alice --conf spark.admin.acls=ops \
  probe.py ch71-acls --hold 15 > "$W/acl.out" 2> "$W/acl.err" & APP=$!
wait_ui http://localhost:4151/api/v1/applications
sleep 1
for u in "" alice bob ops "$(id -un)"; do
  for path in /jobs/ /api/v1/applications/; do
    h=(); [[ -n $u ]] && h=(-H "X-Book-User: $u")
    printf '%-14s GET %-22s -> %s\n' "${u:-(no user)}" "$path" \
      "$(curl -s -o /dev/null -w '%{http_code}' "${h[@]}" "http://localhost:4151$path")"
  done
done | sed "s/^$(id -un)  /owner/"
grep -o "ui acls enabled; users with view permissions: [^;]*; users with modify permissions: [^;]*" "$W/acl.err" | head -1 | sed "s/$(id -un)/<owner>/g"
wait $APP
