#!/usr/bin/env bash
# Build the taxi app in the Rust image (Chapter 82's Dockerfile) and run it against the server of Chapter 82.
#   ch83/run.sh build | app | crossover | resilience
set -euo pipefail
cd "$(dirname "$0")"
S=${SCRATCH:-/tmp/ch82-build}; mkdir -p "$S/cargo" "$S/target83"
DATA=${BOOK_DATA:-$HOME/spark-masterclass/data}/nyc-taxi
RUN="docker run --rm --network host -e TRIPS=$DATA/yellow-h1 -e ZONES=/data/taxi_zone_lookup.csv -v $DATA:/data:ro -v $S/target83:/target book-rust:1.99"
case ${1:-app} in
  build) docker build -q -t book-rust:1.99 -f ../ch82/docker/rust.Dockerfile ../ch82/docker >/dev/null
         docker run --rm -u "$(id -u):$(id -g)" -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR=/target -v "$S/cargo:/cargo" \
           -v "$S/target83:/target" -v "$PWD/app:/app" -w /app book-rust:1.99 cargo build --release ;;
  app) $RUN /target/release/taxi-app | tee output/app.txt ;;
  show) $RUN /target/release/taxi-app ;;                       # the same, without saving (for the film)
  crossover) flock "$DATA/../.bench.lock" $RUN /target/release/crossover | tee output/crossover.txt ;;
  resilience) SCRATCH=$S ./resilience.sh | tee output/resilience.txt ;;
esac
