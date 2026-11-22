#!/usr/bin/env bash
# PostgreSQL 16 in Docker for Chapter 88. It logs every statement, so the chapter can show
# the exact SQL that Spark sends.
#   ch88/pg.sh up       start PostgreSQL on port 15515 (database "book", user "book")
#   ch88/pg.sh psql ... run psql in the container
#   ch88/pg.sh log N    the last N statements in the server log
#   ch88/pg.sh down     remove the container
# The password is a local test password for this container only.
set -euo pipefail
NAME=book-ch88-pg
case "${1:-}" in
  up)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" -p 15515:5432 \
      -e POSTGRES_USER=book -e POSTGRES_PASSWORD=book-secret -e POSTGRES_DB=book \
      postgres:16 -c log_statement=all -c log_line_prefix='%m [%p] ' \
      -c shared_buffers=512MB -c max_wal_size=4GB >/dev/null
    for _ in $(seq 60); do docker exec "$NAME" pg_isready -U book -d book -q && break; sleep 1; done
    sleep 2
    ;;
  psql) shift; docker exec -i "$NAME" psql -U book -d book "$@" ;;
  log) docker logs "$NAME" 2>&1 | tail -n "${2:-40}" ;;
  down) docker rm -f "$NAME" >/dev/null 2>&1 || true ;;
  *) echo "usage: $0 up|psql ...|log N|down"; exit 1 ;;
esac
