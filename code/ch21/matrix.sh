#!/usr/bin/env bash
# Run the spark-connect-matrix harness (github.com/lisancao/spark-connect-matrix, a local copy) for the
# PySpark and Scala clients at 4.3.0 against this chapter's server, and save the run files in output/matrix.
# The copy needs a "4.3.0-book" entry in data/servers.json: url sc://localhost:15106, env pyarrow, pandas, python=3.12.
set -euo pipefail
M=${MATRIX:?set MATRIX to a copy of spark-connect-matrix}; cd "$M"
TZ=UTC uv run -q --isolated --no-project --python 3.12 --with pyspark-client==4.3.0 --with "pandas<3" --with pyarrow \
  --with unittest-xml-reporting python harness/run.py --binding pyspark --remote sc://localhost:15106 --out "$M/out"
SCALA_CLIENT_VERSION=4.3.0 TZ=UTC uv run -q --isolated --no-project --with unittest-xml-reporting \
  python harness/run.py --binding scala --remote sc://localhost:15106 --out "$M/out"
cd - >/dev/null
cp "$M/out/pyspark-client/4.3.0/4.3.0-book.json" output/matrix/pyspark-4.3.0-on-4.3.0.json
cp "$M/out/spark-connect-scala/4.3.0/4.3.0-book.json" output/matrix/scala-4.3.0-on-4.3.0.json
sed -i "s#$HOME#~#g" output/matrix/*.json
python3 matrix_summary.py | tee output/matrix-summary.txt
