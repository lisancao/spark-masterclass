#!/usr/bin/env bash
# Download the NYC TLC data the book uses into data/ at the repo root.
# The months are fixed so that every number printed in the book reproduces.
# Source: NYC Taxi and Limousine Commission trip records
# https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
DATA="$ROOT/data/nyc-taxi"
BASE=https://d37ci6vzurychx.cloudfront.net
mkdir -p "$DATA/yellow"
for month in ${MONTHS:-2026-06}; do
  f="$DATA/yellow/yellow_tripdata_$month.parquet"
  [ -s "$f" ] || curl -fsSL -o "$f" "$BASE/trip-data/yellow_tripdata_$month.parquet"
done
[ -s "$DATA/taxi_zone_lookup.csv" ] || curl -fsSL -o "$DATA/taxi_zone_lookup.csv" "$BASE/misc/taxi_zone_lookup.csv"
cd "$DATA" && find . -type f | sort | xargs sha256sum | tee SHA256SUMS.local
