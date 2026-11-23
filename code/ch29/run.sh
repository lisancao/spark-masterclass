#!/usr/bin/env bash
# Run every step of Chapter 29 in an environment with grpcio, protobuf and pyarrow only (no pyspark),
# and save the outputs. Steps 1-5 use the normal server; step 6 restarts it with small streams.
set -euo pipefail
cd "$(dirname "$0")"
export BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data}
PY="uv run -q --isolated --no-project --with-requirements requirements.txt python"   # no pyspark in this env
hide() { sed "s#$HOME#~#g"; }
[[ -d build/gen ]] || ./gen.sh >/dev/null
$PY -c "import importlib.util as u; print('pyspark importable:', u.find_spec('pyspark') is not None)" > output/env.txt
$PY -c "import grpc, google.protobuf, pyarrow; print('grpcio', grpc.__version__, '| protobuf', google.protobuf.__version__, '| pyarrow', pyarrow.__version__)" >> output/env.txt
../ch27/server.sh start >/dev/null; until nc -z localhost 15106 2>/dev/null; do sleep 1; done
for s in step1_bytes step2_execute step3_arrow_bytes step4_dataframe step5_errors; do $PY $s.py 2>&1 | hide > output/$s.txt; done
../ch27/server.sh small-streams >/dev/null; until nc -z localhost 15106 2>/dev/null; do sleep 1; done
$PY step6_reattach.py 2>&1 | hide > output/step6_reattach.txt
../ch27/server.sh start >/dev/null; until nc -z localhost 15106 2>/dev/null; do sleep 1; done
cat output/env.txt
