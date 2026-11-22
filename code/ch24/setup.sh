#!/usr/bin/env bash
# Chapter 24: download what the engine runs need, outside git, into $BOOK_DATA/ch24.
#   - twelve months of yellow-taxi trips (2025-07 to 2026-06), about 830 MB
#   - the newest release of each engine, checked against the published checksums
#   - one Python venv for each older Spark version that an engine needs
# Run from code/:  BOOK_DATA=~/spark-masterclass/data bash ch24/setup.sh
set -euo pipefail
DATA=${BOOK_DATA:?set BOOK_DATA}/ch24
mkdir -p "$DATA/yellow" "$DATA/jars"
CF=https://d37ci6vzurychx.cloudfront.net/trip-data
for m in 2025-07 2025-08 2025-09 2025-10 2025-11 2025-12 \
         2026-01 2026-02 2026-03 2026-04 2026-05; do
  f="$DATA/yellow/yellow_tripdata_$m.parquet"
  [ -s "$f" ] || curl -fsSL -o "$f" "$CF/yellow_tripdata_$m.parquet"
done
# June 2026 is the book's own month: link it, do not download it again.
ln -sfn ../../nyc-taxi/yellow/yellow_tripdata_2026-06.parquet \
        "$DATA/yellow/yellow_tripdata_2026-06.parquet"

cd "$DATA/jars"
MVN=https://repo1.maven.org/maven2
get_maven() {   # get_maven <group path> <artifact> <version> [classifier]
  local jar="$2-$3${4:+-$4}.jar"
  [ -s "$jar" ] || curl -fsSL -O "$MVN/$1/$2/$3/$jar"
  curl -fsSL -o "$jar.sha1" "$MVN/$1/$2/$3/$jar.sha1"
  echo "$(cat "$jar.sha1")  $jar" | sha1sum -c -
}
# Apache DataFusion Comet 1.2.0, built for Spark 4.2 (no Spark 4.3 build exists).
get_maven org/apache/datafusion comet-spark-spark4.2_2.13 1.2.0
# NVIDIA RAPIDS Accelerator 26.10.0 for CUDA 13 (supports Spark up to 4.2.0).
get_maven com/nvidia rapids-4-spark_2.13 26.10.0 cuda13
# Apache Gluten 1.7.0 with Velox, the binary release for Spark 4.1 (built on 4.1.1).
G=apache-gluten-1.7.0-bin-spark-4.1.tar.gz
[ -s "$G" ] || curl -fsSL -O "https://downloads.apache.org/gluten/1.7.0/$G"
curl -fsSL -o "$G.sha512" "https://downloads.apache.org/gluten/1.7.0/$G.sha512"
sha512sum -c "$G.sha512"
mkdir -p gluten-1.7.0 && tar xzf "$G" -C gluten-1.7.0

# One venv per older Spark: the engines above do not run on 4.3.0 (see the chapter).
for v in 4.1.1 4.2.0; do
  [ -x "$DATA/venv-$v/bin/python" ] || {
    uv venv -q -p 3.12 "$DATA/venv-$v"
    uv pip install -q -p "$DATA/venv-$v/bin/python" "pyspark==$v" "pandas>=2.2,<3" pyarrow
  }
done
echo "ready: $DATA"
