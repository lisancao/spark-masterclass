#!/usr/bin/env bash
# Time the taxi query in each client: one warmup run, then five runs; print the median of the
# "wall time" each program prints (from the start of main to the end of show(), connection included).
# Run under the bench lock:  flock $BOOK_DATA/.bench.lock ch82/time_taxi.sh
set -euo pipefail
cd "$(dirname "$0")"
S=${SCRATCH:-/tmp/ch82-build}
TRIPS=${BOOK_DATA:-$HOME/spark-masterclass/data}/nyc-taxi/yellow
RUN="docker run --rm --network host -e TRIPS=$TRIPS"
U="-u $(id -u):$(id -g)"
declare -A CMD=(
  [python]="uv run --project .. python taxi_python.py"
  [scala]="$RUN $U -e HOME=/home/u -e COURSIER_CACHE=/home/u/.cache/coursier/v1 -e COURSIER_ARCHIVE_CACHE=/home/u/.cache/coursier/arc -v $S/scala-home:/home/u -v $PWD/scala:/app -w /app virtuslab/scala-cli:latest run --server=false --jvm 17 -q Taxi.scala"
  [rust]="$RUN -v $S/target:/target book-rust:1.99 /target/release/taxi"
  [go]="$RUN -v $S/bin:/out golang:latest /out/taxi-go"
  [swift]="$RUN -v $S/swift-build:/build swift:latest /build/release/Taxi"
)
printf '%-8s %s\n' client "median wall time of 5 runs (s), after one warmup"
for c in python scala rust go swift; do
  ts=()
  for i in 0 1 2 3 4 5; do
    t=$(${CMD[$c]} 2>/dev/null | awk '/^wall time/ {print $3}')
    [[ $i -gt 0 ]] && ts+=("$t")
  done
  printf '%-8s %s   (runs: %s)\n' "$c" "$(printf '%s\n' "${ts[@]}" | sort -n | sed -n 3p)" "${ts[*]}"
done
