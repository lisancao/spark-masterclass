#!/usr/bin/env bash
# Chapter 85: a standalone Hive Metastore 4.0.1 in Docker, for ch85/remote_metastore.py.
#   ch85/standalone_metastore.sh up     # start it on localhost:${HMS_PORT:-19113}
#   ch85/standalone_metastore.sh down   # stop and remove it
# The warehouse folder is mounted at the same path inside the container, so that a path that the
# metastore writes (a table folder) is the same path that Spark writes.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
W=$root/data/work/ch85-hms
NAME=sm-ch85-hms
PORT=${HMS_PORT:-19113}
case "${1:-up}" in
  up)
    mkdir -p "$W/warehouse" && chmod 777 "$W/warehouse"
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" -p "$PORT:9083" \
      -e SERVICE_NAME=metastore \
      -e SERVICE_OPTS="-Dhive.metastore.warehouse.dir=$W/warehouse" \
      -v "$W/warehouse:$W/warehouse" apache/hive:4.0.1 >/dev/null
    for _ in $(seq 60); do
      if docker logs "$NAME" 2>&1 | grep -q "Starting Hive Metastore Server"; then break; fi
      sleep 2
    done
    sleep 5
    echo "metastore: thrift://localhost:$PORT  ($(docker inspect -f '{{.Config.Image}}' "$NAME"))"
    docker exec "$NAME" /opt/hive/bin/hive --version 2>/dev/null | head -1 ;;
  down)
    docker rm -f "$NAME" >/dev/null && echo "removed $NAME" ;;
esac
