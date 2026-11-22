#!/usr/bin/env bash
# Chapter 34: the reference pipeline as it runs in production: one long-running Spark Connect
# server, and the CLI as its client with --remote. Trace, dry run, two runs, a full refresh.
# Run from code/:  ch34/end_to_end.sh   (writes ch34/output/*.txt)
set -uo pipefail
cd "$(dirname "$0")/.."
CODE=$PWD; O=ch34/output; mkdir -p $O
W="${BOOK_DATA:-../data}/work/part06/ch34-shop"; PORT=${CONNECT_PORT:-15056}
rm -rf "$W"; cp -r ch34/shop "$W"; cp -r data/retail "$W/data"; sed -i "s#@WORK@#$W#g" "$W/spark-pipeline.yml"
export SPARK_LOCAL_IP=127.0.0.1 SPARK_HOME="$(.venv/bin/python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"

# 1. Start the server, as an operator would (sbin/start-connect-server.sh does the same).
cd "$W"; : > server.log
"$SPARK_HOME/bin/spark-submit" --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --master "local[4]" --conf spark.connect.grpc.binding.port=$PORT \
  --conf spark.ui.port=${SPARK_UI_PORT:-4126} --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.session.timeZone=UTC spark-internal >> server.log 2>&1 &
SERVER=$!; trap 'kill $SERVER 2>/dev/null; wait $SERVER 2>/dev/null' EXIT
until grep -q "Spark Connect server started" server.log; do sleep 1; kill -0 $SERVER || exit 1; done
cd "$CODE"
cli() { uv run --quiet --project "$CODE" --with pyyaml spark-pipelines --remote "sc://localhost:$PORT" "$@" --spec "$W/spark-pipeline.yml" 2>&1 \
        | grep -v -E 'WARN|^\s*$' | sed -e "s#$HOME#~#g" -E -e 's/spark_catalog\.default\.//g'; }
events() { grep -E 'has (COMPLETED|FAILED)|SKIPPED|EXCLUDED|Run is|Error' | sed -E 's/^[0-9-]+ //'; }
mark() { echo "=== $1" >> "$W/server.log"; }

# 2. What the CLI sends: a dry run through the tracer.
{ echo '$ SPARK_REMOTE=sc://localhost:'$PORT' python ch34/trace_cli.py spark-pipeline.yml dry-run'
  SPARK_REMOTE="sc://localhost:$PORT" uv run --quiet --project "$CODE" --with pyyaml python ch34/trace_cli.py "$W/spark-pipeline.yml" dry-run 2>&1 \
    | grep -E '^  ->|Run is|Starting run' | sed -e "s#$HOME#~#g" -E -e 's/^[0-9-]+ [0-9:]+: //'
} > $O/trace.txt

# 3. Run 1, day 3 arrives, run 2, then a full refresh of everything.
mark "run 1"
{ echo "== run 1"; cli run | events
  ls "$W/exports/shipping_feed" | grep -v -E '^\.|_spark_metadata'
  cat "$W"/exports/shipping_feed/*.json | wc -l | sed 's/^/rows in the feed: /'
  cp "$W/data/later/orders_2026-10-03.json" "$W/data/orders/"
  mark "run 2"
  echo; echo "== run 2, after orders_2026-10-03.json arrived"; cli run | events
  cat "$W"/exports/shipping_feed/*.json | wc -l | sed 's/^/rows in the feed: /'
  mark "run 3"
  echo; echo "== run --full-refresh-all"; cli run --full-refresh-all | events
  cat "$W"/exports/shipping_feed/*.json | wc -l | sed 's/^/rows in the feed: /'
  echo "checkpoint versions of the sink flow: $(ls "$W"/pipeline-storage/_checkpoints/shipping_feed/shipped_orders_to_feed | xargs)"
  echo "batches in the sink log exports/shipping_feed/_spark_metadata: $(ls "$W"/exports/shipping_feed/_spark_metadata | grep -v crc | xargs)"
} > $O/runs.txt

# 4. Stop the server (the Derby metastore allows one process), then read the tables.
kill $SERVER; wait $SERVER 2>/dev/null; trap - EXIT
.venv/bin/python ch30/show_tables.py "$W" bronze_orders silver_orders 2>/dev/null | grep rows >> $O/runs.txt

# 5. What the server logged for the pipeline, in run 2.
sed -n '/=== run 2/,/=== run 3/p' "$W/server.log" | grep -E 'pipelines\.|Materializ|checkpoint|Starting flow|TRUNCATE' \
  | sed -E "s#$HOME#~#g; s/^[0-9/]+ [0-9:]+ //; s/spark_catalog\.default\.//g" | cut -c1-150 > $O/server_log.txt
grep -E 'Materializing|Starting flow|using checkpoint' $O/server_log.txt | sed -E 's/`spark_catalog`.`default`.//; s#file:~/spark-masterclass/data/work/part06/ch34-shop/##' | cut -c1-110 > $O/server_log_short.txt
