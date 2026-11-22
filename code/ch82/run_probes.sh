#!/usr/bin/env bash
# Build and run every client's taxi query and capability probe against the Connect server.
# Rust, Go and Swift build and run in Docker (nothing is installed on the host); Scala runs in scala-cli's image.
#   ch82/run_probes.sh <label>     writes output/<client>-taxi.txt and output/<client>-probe-<label>.txt
set -euo pipefail
cd "$(dirname "$0")"
LABEL=${1:-default}
S=${SCRATCH:-/tmp/ch82-build}; mkdir -p "$S"/{cargo,target,gopath,gocache,swift-build,swift-cache,scala-home,bin}
export WORK=$(cd ../.. && pwd)/data/work/ch82; W=$WORK; mkdir -p "$W"
TRIPS=${BOOK_DATA:-$HOME/spark-masterclass/data}/nyc-taxi/yellow
U="-u $(id -u):$(id -g)"
RUN="docker run --rm --network host -e TRIPS=$TRIPS -e WORK=$W -e SPARK_REMOTE=${SPARK_REMOTE:-sc://localhost:15106}"
hide() { sed "s#$HOME#~#g"; }

docker build -q -t book-rust:1.99 -f docker/rust.Dockerfile docker >/dev/null
docker run --rm $U -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR=/target -v "$S/cargo:/cargo" -v "$S/target:/target" \
  -v "$PWD/rust:/app" -w /app book-rust:1.99 cargo build --release -q
docker run --rm $U -e GOPATH=/gopath -e GOCACHE=/gocache -e HOME=/tmp -v "$S/gopath:/gopath" -v "$S/gocache:/gocache" \
  -v "$PWD/go:/app" -v "$S/bin:/out" -w /app golang:latest sh -c 'go build -o /out/taxi-go . && go build -o /out/probe-go ./cmd/probe'
docker run --rm $U -e HOME=/tmp/home -v "$S/swift-cache:/tmp/home" -v "$S/swift-build:/build" -v "$PWD/swift:/app" -w /app \
  swift:latest swift build -c release --scratch-path /build >/dev/null
SCALA="$RUN $U -e HOME=/home/u -e COURSIER_CACHE=/home/u/.cache/coursier/v1 -e COURSIER_ARCHIVE_CACHE=/home/u/.cache/coursier/arc -v $S/scala-home:/home/u -v $PWD/scala:/app -w /app virtuslab/scala-cli:latest run --server=false --jvm 17 -q"

for what in taxi probe; do
  out() { if [[ $what == taxi ]]; then echo "output/$1-taxi.txt"; else echo "output/$1-probe-$LABEL.txt"; fi; }
  py=$([[ $what == taxi ]] && echo taxi_python.py || echo probe_python.py)
  (cd .. && uv run python ch82/$py 2>/dev/null) | hide > "$(out python)"
  $SCALA "$([[ $what == taxi ]] && echo Taxi.scala || echo Probe.scala)" 2>/dev/null | grep -v '^Using Spark' | hide > "$(out scala)"
  $RUN -v "$S/target:/target" book-rust:1.99 /target/release/$what | hide > "$(out rust)"
  $RUN -v "$S/bin:/out" golang:latest /out/$what-go | hide > "$(out go)"
  $RUN -v "$S/swift-build:/build" swift:latest /build/release/$([[ $what == taxi ]] && echo Taxi || echo Probe) | hide > "$(out swift)"
done
tail -n +1 output/*-probe-$LABEL.txt
