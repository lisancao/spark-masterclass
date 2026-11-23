#!/usr/bin/env bash
# MinIO in Docker: an S3-compatible object store on this machine, for Chapter 87.
#   ch87/minio.sh up     start MinIO on port 19055 (console 19056) and make the bucket "book"
#   ch87/minio.sh mc ... run an mc command against it (alias "m")
#   ch87/minio.sh down   remove the container and its data
# The keys are local test keys for this container only. No cloud account is used.
set -euo pipefail
NAME=book-ch87-minio
IMG=minio/minio:RELEASE.2025-04-08T15-41-24Z
MC=minio/mc:RELEASE.2025-04-08T15-39-49Z
USER_=bookadmin
PASS=bookadmin-secret
case "${1:-}" in
  up)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" -p 19055:9000 -p 19056:9001 \
      -e MINIO_ROOT_USER=$USER_ -e MINIO_ROOT_PASSWORD=$PASS \
      "$IMG" server /data --console-address :9001 >/dev/null
    for _ in $(seq 30); do curl -sf http://localhost:19055/minio/health/live && break; sleep 1; done
    "$0" mc mc mb --ignore-existing m/book
    ;;
  mc)
    shift
    docker run --rm -i --network host --entrypoint sh "$MC" \
      -c "mc alias set m http://localhost:19055 $USER_ $PASS >/dev/null && $*"
    ;;
  down)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    ;;
  *) echo "usage: $0 up|mc ...|down"; exit 1 ;;
esac
