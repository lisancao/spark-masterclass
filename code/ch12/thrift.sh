#!/usr/bin/env bash
# Chapter 12: start or stop the Spark Thrift JDBC/ODBC server from the pip install.
# The pip package has the server class but not sbin/start-thriftserver.sh, so this calls
# spark-submit the way that script does. It uses the metastore of ch12/sql_cli.sh.
#   ch12/thrift.sh start | stop      (port $THRIFT_PORT, default 15102; UI on $SPARK_UI_PORT)
set -euo pipefail
cd "$(dirname "$0")/.."
W="${BOOK_DATA:-$PWD/../data}/work/ch12-cli"
PORT=${THRIFT_PORT:-15102}
PIDF="$W/thrift.pid"
if [[ ${1:-start} == stop ]]; then
  if [[ -f $PIDF ]]; then
    pid=$(cat "$PIDF"); kill "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null; do sleep 1; done
    rm -f "$PIDF"
  fi
  exit 0
fi
if (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; then
  echo "port $PORT is in use; stop that server first" >&2; exit 1
fi
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOCAL_IP=127.0.0.1
cd "$W"
"$SPARK_HOME/bin/spark-submit" --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
  --name "Thrift JDBC/ODBC Server" --master "local[2]" \
  --conf spark.ui.port=${SPARK_UI_PORT:-4142} \
  --conf "spark.sql.warehouse.dir=$W/warehouse" \
  --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$W/metastore_db;create=true" \
  --hiveconf hive.server2.thrift.port=$PORT --hiveconf hive.server2.thrift.bind.host=127.0.0.1 \
  spark-internal > "$W/thrift.log" 2>&1 &
echo $! > "$PIDF"
until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
  kill -0 "$(cat "$PIDF")" 2>/dev/null || { tail -20 "$W/thrift.log"; exit 1; }
  sleep 1
done
echo "Thrift server up: jdbc:hive2://localhost:$PORT, UI http://localhost:${SPARK_UI_PORT:-4142}"
