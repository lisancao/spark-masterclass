#!/usr/bin/env bash
# Chapter 12: the Thrift server with two clients, beeline (JDBC) and PyHive (Python).
# Needs the metastore from ch12/sql_cli.sh. Starts and stops the server.
# Run from code/:  ch12/thrift_demo.sh > ch12/output/thrift_demo.txt
set -euo pipefail
cd "$(dirname "$0")/.."
PORT=${THRIFT_PORT:-15102}
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
ch12/thrift.sh start
trap 'ch12/thrift.sh stop' EXIT
bl() { "$SPARK_HOME/bin/beeline" -u "jdbc:hive2://localhost:$PORT" -n "$USER" --silent=true \
         --showHeader=true --outputformat=table "$@" 2>&1 | grep -v -E '^WARNING|^\s*$' ; }
echo "== 1. beeline, a JDBC client"
echo "\$ beeline -u jdbc:hive2://localhost:$PORT -n \$USER -e \"...\""
bl -e "SHOW TABLES IN taxi;
SELECT Borough, count(*) AS zones FROM taxi.zones GROUP BY Borough ORDER BY zones DESC LIMIT 3;"
echo
echo "== 2. each connection is its own session"
bl -e "CREATE TEMPORARY VIEW mine AS SELECT 1 AS x; SHOW VIEWS;"
echo "-- a second connection:"
bl -e "SHOW VIEWS;"
echo
echo "== 3. PyHive, a Python client with no JVM"
uv run --quiet --with 'pyhive[hive_pure_sasl]' python ch12/thrift_client.py
