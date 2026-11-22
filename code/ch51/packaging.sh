#!/usr/bin/env bash
# Chapter 51: install pyspark-mailbox into its own virtual environment, run its tests, and see
# what the workers' Python, a broken pyspark_* package and Spark Connect do to the lookup.
#   cd code && ch51/packaging.sh            (needs uv; makes two venvs under $CH51_VENVS)
set -uo pipefail
cd "$(dirname "$0")/.."
CODE=$PWD
export CH51_VENVS=${CH51_VENVS:-${TMPDIR:-/tmp}/ch51-venvs}
V=$CH51_VENVS
export SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=${SPARK_UI_PORT:-4029}
short() { sed "s#$HOME#~#g; s#$V#\$CH51_VENVS#g"; }

echo "== Two virtual environments: 'with' has the package, 'bare' has only pyspark"
for env in with bare; do
  [ -x "$V/$env/bin/python" ] || uv venv -q --python 3.12 "$V/$env"
  VIRTUAL_ENV=$V/$env uv pip install -q "pyspark[connect]==4.3.0" pytest
done
VIRTUAL_ENV=$V/with uv pip install -q -e ch51/pyspark-mailbox
VIRTUAL_ENV=$V/with uv pip list 2>/dev/null | grep -E "^(pyspark|pyspark-mailbox) " | short

echo; echo "== Plain-Python tests: no SparkSession, no JVM"
(cd ch51/pyspark-mailbox && "$V/with/bin/python" -m pytest -q -p no:cacheprovider tests 2>&1 | tail -2)

echo; echo "== 1. The driver uses 'with', PYSPARK_PYTHON is not set"
"$V/with/bin/python" ch51/registration.py lookup 2>/dev/null | short

echo; echo "== 2. Driver and workers both use 'with'"
PYSPARK_PYTHON=$V/with/bin/python "$V/with/bin/python" ch51/registration.py classic 2>/dev/null | short

echo; echo "== 3. The driver uses 'with', the workers use 'bare'"
PYSPARK_PYTHON=$V/bare/bin/python "$V/with/bin/python" ch51/registration.py lookup 2>/dev/null | short

echo; echo "== 4. A second pyspark_* package that fails to import"
VIRTUAL_ENV=$V/with uv pip install -q -e ch51/pyspark-broken
CH51_WARN=1 PYSPARK_PYTHON=$V/with/bin/python "$V/with/bin/python" ch51/registration.py lookup 2>"$V/stderr.txt" | short
grep -o "Skipping the lookup of Python Data Sources[^:]*" "$V/stderr.txt" | head -1; grep -o "ModuleNotFoundError: [^\\]*" "$V/stderr.txt" | head -1
VIRTUAL_ENV=$V/with uv pip uninstall -q pyspark-broken

echo; echo "== 5. Spark Connect: the server's workers use 'bare', the client uses 'with'"
SH=$("$V/bare/bin/python" -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_HOME=$SH SPARK_LOG_DIR=$CODE/../data/logs-part09a SPARK_PID_DIR=$CODE/../data/pids-part09a
export SPARK_IDENT_STRING=part09a-ch51
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SH/sbin/stop-connect-server.sh" >/dev/null 2>&1
PYSPARK_PYTHON=$V/bare/bin/python "$SH/sbin/start-connect-server.sh" \
  --conf spark.connect.grpc.binding.port=10290 --conf spark.ui.port=4129 >/dev/null
until curl -sf localhost:4129/api/v1/applications >/dev/null; do sleep 1; done
"$V/with/bin/python" ch51/registration.py connect sc://localhost:10290 2>/dev/null | short
"$SH/sbin/stop-connect-server.sh" >/dev/null 2>&1
