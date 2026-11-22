#!/usr/bin/env bash
# Start or stop a Spark 4.3 Connect server with this chapter's jars and plugins.
# gRPC on 15060, the UI and its REST API on 4160 (so it can run next to other servers).
#   ch54/server.sh start      plugin jar on the classpath (spark.jars), plugins configured
#   ch54/server.sh bare       plugins configured, but the plugin jar NOT on the classpath
#   ch54/server.sh stop
set -euo pipefail
cd "$(dirname "$0")/.."
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOG_DIR=$PWD/../data/logs-ch54 SPARK_PID_DIR=$PWD/../data/pids-ch54 SPARK_LOCAL_IP=127.0.0.1
mkdir -p "$SPARK_LOG_DIR" "$SPARK_PID_DIR"
"$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
[[ ${1:-start} == stop ]] && exit 0
JARS=$PWD/ch52/jdocs/target/jdocs-0.1.0.jar,$PWD/ch53/jext/target/jext-0.1.0.jar
[[ ${1:-start} == start ]] && JARS=$JARS,$PWD/ch54/jplug/target/jplug-0.1.0.jar
"$SPARK_HOME/sbin/start-connect-server.sh" \
  --jars "$JARS" \
  --conf spark.connect.grpc.binding.port=15060 \
  --conf spark.ui.port=4160 \
  --conf spark.sql.extensions=jext.Extensions \
  --conf spark.connect.extensions.relation.classes=jplug.PlanNodesPlugin \
  --conf spark.connect.extensions.expression.classes=jplug.MagicPlugin \
  --conf spark.connect.extensions.command.classes=jplug.NotePlugin >/dev/null
until curl -sf localhost:4160/api/v1/applications >/dev/null; do sleep 1; done
echo "Connect server up ($1): sc://localhost:15060, UI http://localhost:4160"
