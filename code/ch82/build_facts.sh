#!/usr/bin/env bash
# One clean release build of each compiled client (dependency downloads cached, compile not),
# then the size of each binary. A single run each: the times are for orientation only.
set -euo pipefail
cd "$(dirname "$0")"
S=${SCRATCH:-/tmp/ch82-build}; F=$S/facts; rm -rf "$F"; mkdir -p "$F"/{target,gocache,swift-build}
U="-u $(id -u):$(id -g)"
t() { local s=$(date +%s.%N); "$@" >/dev/null 2>&1; echo "$(date +%s.%N) - $s" | bc; }
rt=$(t docker run --rm $U -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR=/target -v "$S/cargo:/cargo" -v "$F/target:/target" \
  -v "$PWD/rust:/app" -w /app book-rust:1.99 cargo build --release --bin taxi)
gt=$(t docker run --rm $U -e GOPATH=/gopath -e GOCACHE=/gocache -e HOME=/tmp -v "$S/gopath:/gopath" -v "$F/gocache:/gocache" \
  -v "$PWD/go:/app" -v "$F:/out" -w /app golang:latest go build -o /out/taxi-go .)
st=$(t docker run --rm $U -e HOME=/tmp/home -v "$S/swift-cache:/tmp/home" -v "$F/swift-build:/build" -v "$PWD/swift:/app" -w /app \
  swift:latest swift build -c release --scratch-path /build --product Taxi)
size() { du -m "$1" | cut -f1; }
printf '%-6s %-18s %s\n' client "clean build (s)" "binary (MB)"
printf '%-6s %-18.0f %s\n' rust "$rt" "$(size $F/target/release/taxi)"
printf '%-6s %-18.0f %s\n' go "$gt" "$(size $F/taxi-go)"
printf '%-6s %-18.0f %s\n' swift "$st" "$(size $F/swift-build/release/Taxi)"
