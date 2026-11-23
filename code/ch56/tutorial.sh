#!/usr/bin/env bash
# Chapter 56: your first ETL pipeline, from an empty folder to a second run and a broken file.
# Each step prints the commands it runs ("$ ...") and their output into output/<step>.txt.
#   BOOK_DATA=~/spark-masterclass/data SPARK_UI_PORT=4144 CONNECT_PORT=15104 bash ch56/tutorial.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HERE/output"; mkdir -p "$OUT"
BOOK_DATA="${BOOK_DATA:-$HOME/spark-masterclass/data}"; export BOOK_DATA
PORT="${CONNECT_PORT:-15104}"; UI="${SPARK_UI_PORT:-4144}"
W="$BOOK_DATA/work/sdp/ch56"
export SPARK_LOCAL_IP=127.0.0.1 SPARK_PID_DIR="$W/run" SPARK_LOG_DIR="$W/run"
rm -rf "$W"; mkdir -p "$W"; cd "$W"

clean() { sed -u -e "s#$HOME#~#g" -e 's/\r.*\r//' | grep --line-buffered -v -E '^\s+at |WARN|^Using Spark|^To adjust|^Setting default log|^WARNING: Using incubator|FutureWarning|^\s*$'; }
step() { exec > >(clean > "$OUT/$1.txt") 2>&1; }
x() { echo "\$ $*"; eval "$@" 2>&1; local rc=$?; echo; return $rc; }

step 01-install
x 'python3 -m venv .venv'
x 'source .venv/bin/activate'
x 'pip install --quiet "pyspark[pipelines]==4.3.0" "pandas<3"'
x 'pip list 2>/dev/null | grep -i -E "^(pyspark|pyyaml|grpcio |pandas|pyarrow) "'
x 'spark-pipelines --help'

step 02-init
x 'spark-pipelines init --name taxi_etl'
x 'cd taxi_etl'
x 'find . -type f | sort'
x 'cat spark-pipeline.yml'
x 'rm transformations/example_*'

step 03-data
x 'mkdir -p data'
x 'cp "$BOOK_DATA/nyc-taxi/taxi_zone_lookup.csv" data/'
cp "$HERE/make_days.py" "$HERE/look.py" .   # the two helper scripts of this chapter
x 'python make_days.py data/trips 2026-06-01'
x 'ls -sh data data/trips'

step 04-files
cp "$HERE/project/transformations/"* transformations/
x 'find transformations -type f | sort'

step 05-server
x 'SPARK_HOME=$(python -c "import os, pyspark; print(os.path.dirname(pyspark.__file__))")'
x '$SPARK_HOME/sbin/start-connect-server.sh --conf spark.connect.grpc.binding.port='"$PORT"' --conf spark.sql.catalogImplementation=hive --conf spark.ui.port='"$UI"' | sed "s#$W#.#"'
for _ in $(seq 120); do (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 0.5; done
x 'export SPARK_REMOTE=sc://localhost:'"$PORT"

step 06-dry-run
x 'spark-pipelines dry-run --remote $SPARK_REMOTE; echo "exit code: $?"'

step 07-run1
x 'spark-pipelines run --remote $SPARK_REMOTE; echo "exit code: $?"'

step 08-look1
x 'python look.py $SPARK_REMOTE'

step 09-disk1
x 'ls'
x 'ls spark-warehouse spark-warehouse/clean_trips'
x 'find pipeline-storage -type d | sort'
x 'cat pipeline-storage/_checkpoints/spark_catalog/default/raw_trips/raw_trips/0/sources/0/0'

step 10-day2
x 'python make_days.py data/trips 2026-06-02'
x 'spark-pipelines run --remote $SPARK_REMOTE | grep -E "Flow .*(RUNNING|COMPLETED)|Run is"'
x 'python look.py $SPARK_REMOTE'
x 'ls pipeline-storage/_checkpoints/spark_catalog/default/raw_trips/raw_trips/0/commits'
x 'cat pipeline-storage/_checkpoints/spark_catalog/default/raw_trips/raw_trips/0/sources/0/1'
x 'ls spark-warehouse/raw_trips | grep -c parquet'

step 11-break
x "sed -i 's/avg(c.fare_amount)/avg(c.fare)/' transformations/borough_daily.sql"
x 'grep -n "avg(" transformations/borough_daily.sql'
x 'spark-pipelines run --remote $SPARK_REMOTE; echo "exit code: $?"'
x 'python look.py $SPARK_REMOTE | tail -12'
x "sed -i 's/avg(c.fare)/avg(c.fare_amount)/' transformations/borough_daily.sql"
x 'spark-pipelines run --remote $SPARK_REMOTE | grep -E "Run is"; echo "exit code: ${PIPESTATUS[0]}"'

step 12-stop
x '$SPARK_HOME/sbin/stop-connect-server.sh'
