#!/usr/bin/env bash
# Chapter 22: reattach and retries, seen from the app.
#  1. The server ends each response stream after 1 MiB; the app pulls 71 MiB and counts nothing itself.
#     (resilience.sh wait runs only part 2)
#  2. The server is down when the app starts; it comes up 8 s later; the app's retries carry it through.
set -euo pipefail
cd "$(dirname "$0")"
S=${SCRATCH:-/tmp/ch21-build}
RUN="docker run --rm --network host -e TRIPS=${BOOK_DATA:-$HOME/spark-masterclass/data}/nyc-taxi/yellow-h1 -v $S/target83:/target book-rust:1.99 /target/release/resilience"
LOG() { ls -t ../../data/logs-ch21/*.out | head -1; }
up() { until nc -z localhost 15106 2>/dev/null; do sleep 0.2; done; }

if [[ ${1:-all} != wait ]]; then
echo "== 1. server.sh small-streams: each response stream ends after 1 MiB"
../ch21/server.sh small-streams >/dev/null; up
before=$(wc -l < "$(LOG)")
$RUN pull
sleep 1
n=$(tail -n +$((before + 1)) "$(LOG)" | grep 'ExecuteGrpcResponseSender: Starting for opId' | grep -vc 'lastConsumedStreamIndex=0$' || true)
echo "server log: $n ReattachExecute calls for this pull"
fi

echo "== 2. the server is down when the app starts"
../ch21/server.sh stop
start=$(date +%s.%N)
$RUN wait > ../../data/work/ch22-wait.txt 2>&1 &
app=$!
sleep 8
echo "[$(printf '%5.1f' "$(echo "$(date +%s.%N) - $start" | bc)") s] server.sh start"
../ch21/server.sh start >/dev/null
wait $app
cat ../../data/work/ch22-wait.txt
