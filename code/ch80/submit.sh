#!/usr/bin/env bash
# Chapter 80, Step 2 and Step 3: one application, submitted in the ways that the standalone cluster accepts.
#   ch80/submit.sh verbose | client | cluster-jar | cluster-python      (from code/, cluster up)
set -uo pipefail
cd "$(dirname "$0")"
source ./env.sh
SUBMIT=$SPARK_HOME/bin/spark-submit
TRIPS=$BOOK_DATA/nyc-taxi/yellow
quiet() { grep -v -E "WARNING: Using incubator|NativeCodeLoader|^\s*$"; }
# keep the scheduler lines that show the cluster manager at work, and the lines the app prints
story() { grep -E "StandaloneAppClient|StandaloneSchedulerBackend: (Connected|Granted)|ClientEndpoint|DriverWrapper|^[a-z]" | sed -E 's/^[0-9/]+ [0-9:]+ INFO //'; }
case ${1:-client} in
verbose)          # what spark-submit parsed and what it hands to the child main class
  $SUBMIT --master "$MASTER" --verbose --conf spark.executor.cores=1 --name ch80-verbose \
    app.py "$TRIPS" 2>&1 | quiet | sed -n '/^Main class:/,/^Classpath elements:/p' ;;
client)
  $SUBMIT --master "$MASTER" --deploy-mode client --executor-memory 1g --total-executor-cores 4 \
    app.py "$TRIPS" 2>&1 | story ;;
cluster-python)   # standalone refuses Python in cluster mode
  $SUBMIT --master "$MASTER" --deploy-mode cluster app.py "$TRIPS" 2>&1 | quiet | head -3 ;;
cluster-jar)      # a JVM application: the master starts the driver on a worker
  $SUBMIT --master "$MASTER" --deploy-mode cluster --class org.apache.spark.examples.SparkPi \
    --conf spark.ui.port=4151 --conf spark.standalone.submit.waitAppCompletion=true \
    "$SPARK_HOME/examples/jars/spark-examples_2.13-4.3.0.jar" 200 2>&1 | story
  d=$(ls -td "$SPARK_WORKER_DIR"/driver-* | head -1)
  echo "--- $(basename "$d")/stdout on the worker"; grep -h "Pi is" "$d/stdout" ;;
rest)             # the same JVM application through the master's REST submission server (port 6151)
  $SUBMIT --master spark://127.0.0.1:6151 --deploy-mode cluster --conf spark.master.rest.enabled=true \
    --class org.apache.spark.examples.SparkPi --conf spark.ui.port=4151 \
    "$SPARK_HOME/examples/jars/spark-examples_2.13-4.3.0.jar" 50 2>&1 | grep -E "RestSubmissionClient|\"" \
    | sed -E 's/^[0-9/]+ [0-9:]+ INFO //' | head -30
  id=$(ls -td "$SPARK_WORKER_DIR"/driver-* | head -1 | xargs basename)
  sleep 8
  echo "--- GET /v1/submissions/status/$id"
  curl -s "http://127.0.0.1:6151/v1/submissions/status/$id" ;;
esac
