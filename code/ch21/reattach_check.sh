#!/usr/bin/env bash
# Against a server started with `server.sh small-streams` (each response stream ends after 1 MiB),
# run each client's probe and count, from the server log, how its operations were sent:
#   reattachable=true/false ops, and ReattachExecute calls (a sender that starts at an index > 0).
# Run after run_probes.sh has built everything.
set -euo pipefail
cd "$(dirname "$0")"
S=${SCRATCH:-/tmp/ch21-build}
export WORK=$(cd ../.. && pwd)/data/work/ch21
TRIPS=${BOOK_DATA:-$HOME/spark-masterclass/data}/nyc-taxi/yellow
LOG=$(ls -t ../../data/logs-ch21/*.out | head -1)
RUN="docker run --rm --network host -e TRIPS=$TRIPS -e WORK=$WORK"
U="-u $(id -u):$(id -g)"
declare -A CMD=(
  [python]="uv run --project .. python probe_python.py"
  [scala]="$RUN $U -e HOME=/home/u -e COURSIER_CACHE=/home/u/.cache/coursier/v1 -e COURSIER_ARCHIVE_CACHE=/home/u/.cache/coursier/arc -v $S/scala-home:/home/u -v $PWD/scala:/app -w /app virtuslab/scala-cli:latest run --server=false --jvm 17 -q Probe.scala"
  [rust]="$RUN -v $S/target:/target book-rust:1.99 /target/release/probe"
  [go]="$RUN -v $S/bin:/out golang:latest /out/probe-go"
  [swift]="$RUN -v $S/swift-build:/build swift:latest /build/release/Probe"
)
printf '%-8s %-22s %-14s %-15s %s\n' client big_result "reattachable" "not reattachable" "ReattachExecute calls"
for c in python scala rust go swift; do
  before=$(wc -l < "$LOG")
  big=$(${CMD[$c]} 2>/dev/null | grep big_result | awk '{$1=""; print}' | xargs)
  sleep 2
  new=$(tail -n +$((before + 1)) "$LOG" | grep 'ExecuteGrpcResponseSender: Starting for opId' || true)
  yes=$(grep -c "reattachable=true, lastConsumedStreamIndex=0$" <<<"$new" || true)
  no=$(grep -c 'reattachable=false' <<<"$new" || true)
  re=$(grep -v 'lastConsumedStreamIndex=0$' <<<"$new" | grep -c . || true)
  printf '%-8s %-22s %-14s %-15s %s\n' "$c" "$big" "$yes" "$no" "$re"
done
