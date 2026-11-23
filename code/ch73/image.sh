#!/usr/bin/env bash
# Chapter 73, Step 11: what the official Spark 4.3.0 Python image contains, and a default @udf in it.
#   ch73/image.sh
set -euo pipefail
IMG=apache/spark:4.3.0-python3
cd "$(dirname "$0")"
docker run --rm --entrypoint sh "$IMG" -c '
  . /etc/os-release; echo "os       $PRETTY_NAME"
  echo "python   $(python3 --version 2>&1)"
  echo "java     $(java -version 2>&1 | head -1)"
  echo "user     $(id -un) ($(id -u))"
  echo "pyarrow  $(python3 -c "import pyarrow; print(pyarrow.__version__)" 2>&1 | tail -1)"
  echo "pandas   $(python3 -c "import pandas; print(pandas.__version__)" 2>&1 | tail -1)"
  echo "pythonpath libs: $(ls /opt/spark/python/lib | tr "\n" " ")"'
echo
docker run --rm -v "$PWD/image_udf.py:/tmp/image_udf.py:ro" --entrypoint sh "$IMG" -c \
  '/opt/spark/bin/spark-submit /tmp/image_udf.py 2>/dev/null'
