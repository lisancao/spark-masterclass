#!/usr/bin/env bash
# Take the Spark Connect .proto files from the v4.3.0 tag and compile them to Python modules.
# Needs a Spark git checkout (SPARK_SRC, default ~/repos/spark). No pyspark: only grpcio-tools.
set -euo pipefail
cd "$(dirname "$0")"
SPARK_SRC=${SPARK_SRC:-$HOME/repos/spark}
SRC=sql/connect/common/src/main/protobuf/spark/connect
rm -rf build && mkdir -p build/proto/spark/connect build/gen
for f in $(git -C "$SPARK_SRC" ls-tree --name-only v4.3.0 "$SRC/"); do
  git -C "$SPARK_SRC" show "v4.3.0:$f" > "build/proto/spark/connect/$(basename "$f")"
done
ls build/proto/spark/connect
uv run --isolated --no-project --with-requirements requirements.txt python -m grpc_tools.protoc \
  -I build/proto --python_out=build/gen --grpc_python_out=build/gen build/proto/spark/connect/*.proto
find build/gen -name '*.py' | sort
