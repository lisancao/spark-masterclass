#!/usr/bin/env bash
# Run one prebuilt client program (run_probes.sh builds them) against the Connect server.
#   ch27/client.sh rust|go|swift taxi|probe
set -euo pipefail
cd "$(dirname "$0")"
S=${SCRATCH:-/tmp/ch27-build}
export WORK=$(cd ../.. && pwd)/data/work/ch27
RUN="docker run --rm --network host -e TRIPS=${BOOK_DATA:-$HOME/spark-masterclass/data}/nyc-taxi/yellow -e WORK=$WORK"
case $1-$2 in
  rust-*)    $RUN -v "$S/target:/target" book-rust:1.99 /target/release/$2 ;;
  go-*)      $RUN -v "$S/bin:/out" golang:latest /out/$2-go ;;
  swift-taxi)  $RUN -v "$S/swift-build:/build" swift:latest /build/release/Taxi ;;
  swift-probe) $RUN -v "$S/swift-build:/build" swift:latest /build/release/Probe ;;
esac | sed "s#$HOME#~#g"
