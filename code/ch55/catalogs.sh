#!/usr/bin/env bash
# Chapter 55: start or stop the two catalog servers that the chapter uses.
#   code/ch55/catalogs.sh up     Hive Metastore on :19083, Apache Polaris (REST catalog) on :18181
#   code/ch55/catalogs.sh down   stop and remove both containers
# Each container mounts its warehouse folder at the same path as on the host, so a file:// path
# means the same file to Spark and to the server. Both run as your user, so you own the files.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
WORK=${CH31_WORK:-$(cd "$here/../.." && pwd)/data/ch55}
POLARIS_IMAGE=apache/polaris:1.9.0
HIVE_IMAGE=apache/hive:3.1.3

up() {
  mkdir -p "$WORK/hive" "$WORK/polaris"
  docker run -d --name ch55-hms --user "$(id -u):$(id -g)" -v /etc/passwd:/etc/passwd:ro \
    -v "$WORK/hive:$WORK/hive" -p 19083:9083 \
    -e SERVICE_NAME=metastore \
    -e SERVICE_OPTS="-Dhive.metastore.warehouse.dir=file://$WORK/hive" \
    "$HIVE_IMAGE" >/dev/null
  docker run -d --name ch55-polaris --user "$(id -u):$(id -g)" -v /etc/passwd:/etc/passwd:ro \
    -v "$WORK/polaris:$WORK/polaris" -p 18181:8181 -p 18182:8182 \
    -e POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,s3cr3t \
    -e polaris.realm-context.realms=POLARIS \
    -e 'polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true' \
    -e 'polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["FILE"]' \
    -e polaris.readiness.ignore-severe-issues=true \
    "$POLARIS_IMAGE" >/dev/null
  printf 'waiting for Polaris'
  until curl -sf http://localhost:18182/q/health/ready >/dev/null; do printf .; sleep 2; done; echo
  token=$(curl -s http://localhost:18181/api/catalog/v1/oauth/tokens \
    -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
    -d scope=PRINCIPAL_ROLE:ALL | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
  # A Polaris catalog called "taxi", with its files under $WORK/polaris
  curl -s -o /dev/null -w 'create Polaris catalog taxi: HTTP %{http_code}\n' \
    -X POST http://localhost:18181/api/management/v1/catalogs \
    -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    -d "{\"catalog\": {\"name\": \"taxi\", \"type\": \"INTERNAL\",
         \"properties\": {\"default-base-location\": \"file://$WORK/polaris\"},
         \"storageConfigInfo\": {\"storageType\": \"FILE\", \"allowedLocations\": [\"file://$WORK/polaris\"]}}}"
  printf 'waiting for the metastore'
  until (exec 3<>/dev/tcp/localhost/19083) 2>/dev/null; do printf .; sleep 2; done; echo
  docker ps --filter name=ch55- --format '{{.Names}}  {{.Image}}  {{.Status}}'
}

down() { docker rm -f ch55-hms ch55-polaris >/dev/null 2>&1 || true; echo "ch55 containers removed"; }

"${1:-up}"
