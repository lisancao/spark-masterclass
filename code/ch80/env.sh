# Chapter 80: shared settings for the standalone scripts (source it from code/).
# SPARK_HOME is the 4.3.0 tarball from Chapter 6. Logs, PIDs and recovery state go to a scratch folder.
export BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data}
export SPARK_HOME=$BOOK_DATA/tarball/spark-4.3.0-bin-hadoop3
export CH80_WORK=${CH80_WORK:-$BOOK_DATA/work/ch80}
export SPARK_CONF_DIR=$CH80_WORK/conf SPARK_LOG_DIR=$CH80_WORK/logs SPARK_PID_DIR=$CH80_WORK/pids
export SPARK_WORKER_DIR=$CH80_WORK/worker
export SPARK_LOCAL_IP=127.0.0.1 SPARK_MASTER_HOST=127.0.0.1
export SPARK_MASTER_PORT=7151 SPARK_MASTER_WEBUI_PORT=8151
export SPARK_WORKER_INSTANCES=2 SPARK_WORKER_CORES=2 SPARK_WORKER_MEMORY=2g SPARK_WORKER_WEBUI_PORT=8152
export MASTER=spark://127.0.0.1:7151
export PYSPARK_PYTHON=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.venv/bin/python
