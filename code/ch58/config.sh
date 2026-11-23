#!/usr/bin/env bash
# Chapter 58: how the spec's configuration, catalog and database reach the pipeline, per-environment
# specs, values that are not strings, environment variables, and an Iceberg catalog.
# Output: output/<section>.txt.   CONNECT_PORT=15104 SPARK_UI_PORT=4144 bash ch58/config.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; OUT="$HERE/output"
. "$HERE/../common/sdp.sh"
sdp_venv
W="$BOOK_DATA/work/sdp/ch58"; rm -rf "$W"; mkdir -p "$W"
R="--remote sc://localhost:$PORT"
section() { exec > >(clean > "$OUT/$1.txt") 2>&1; }
trap sdp_stop EXIT
sdp_project "$W/taxi_etl"; P="$W/taxi_etl"
cp "$HERE/probe/probe.py" "$P/transformations/"
sdp_server "$P" --conf spark.sql.catalogImplementation=hive
cd "$P"
ev() { grep -E '^probe:|Exception|Error:|Run is' | sed -E 's/^[0-9-]+ [0-9:]+: //' | cut -c1-170; }
files() { for t in "$@"; do printf '%-18s %s part files\n' "$t" "$(ls spark-warehouse/$t | grep -c '^part-')"; done; }

section defaults
x "spark-pipelines run $R 2>&1 | ev"
x 'files borough_daily zones_by_borough'

section configuration
cat >> spark-pipeline.yml <<'YML'
configuration:
  spark.sql.shuffle.partitions: "3"
  spark.sql.adaptive.enabled: "false"
  taxi.min_fare: "2.50"
YML
x 'cat spark-pipeline.yml'
x "spark-pipelines run $R 2>&1 | ev"
x 'files borough_daily zones_by_borough'

section static
x "echo '  spark.sql.catalogImplementation: \"in-memory\"' >> spark-pipeline.yml && tail -1 spark-pipeline.yml"
x "spark-pipelines run $R 2>&1 | grep -E '^probe: spark.sql.catalog|Failed|Error|Exception|Run is' | sed -E 's/^[0-9-]+ [0-9:]+: //' | cut -c1-150 | uniq -c"
x 'files borough_daily zones_by_borough'
x "sed -i '/catalogImplementation/d' spark-pipeline.yml"

section cli-conf
x "spark-pipelines $R --conf spark.sql.shuffle.partitions=5 --conf taxi.min_fare=9 run 2>&1 | ev"
x "cp spark-pipeline.yml /tmp/sdp-with-conf.yml && sed -i '/^configuration:/,\$d' spark-pipeline.yml"
x "spark-pipelines $R --conf spark.sql.shuffle.partitions=5 --conf taxi.min_fare=9 run 2>&1 | ev"
x "cp /tmp/sdp-with-conf.yml spark-pipeline.yml && rm /tmp/sdp-with-conf.yml"

section not-string
x "sed -i 's/\"3\"/3/' spark-pipeline.yml && grep shuffle spark-pipeline.yml"
x "spark-pipelines run $R 2>&1 | tail -1"
x "sed -i 's/partitions: 3/partitions: \"3\"/' spark-pipeline.yml"

section sql-param
x "sed -i 's/WHERE fare_amount > 0/WHERE fare_amount > \${taxi.min_fare}/' transformations/clean_trips.sql && grep -n WHERE transformations/clean_trips.sql"
x "spark-pipelines run $R --full-refresh clean_trips 2>&1 | grep -E 'Exception|Error|Run is' | cut -c1-200"
x "python - $PORT <<'PY'
import sys
from pyspark.sql import SparkSession
s = SparkSession.builder.remote(f'sc://localhost:{sys.argv[1]}').getOrCreate()
s.sql('SELECT count(*) AS trips, min(fare_amount) AS lowest_fare FROM clean_trips').show()
PY"

x "sed -i 's/taxi.min_fare: \"2.50\"/taxi.min_fare: \"30\"/' spark-pipeline.yml && grep min_fare spark-pipeline.yml"
x "spark-pipelines run $R --full-refresh clean_trips 2>&1 | grep -E 'Exception|Error|Run is' | cut -c1-200"
x "python - $PORT <<'PY'
import sys
from pyspark.sql import SparkSession
s = SparkSession.builder.remote(f'sc://localhost:{sys.argv[1]}').getOrCreate()
s.sql('SELECT count(*) AS trips, min(fare_amount) AS lowest_fare FROM clean_trips').show()
PY"
x "sed -i '/min_fare/d' spark-pipeline.yml"
x "spark-pipelines run $R 2>&1 | grep -E 'Exception|Error' -A3 | cut -c1-160 | head -8"
x "sed -i 's/taxi.min_fare: \"30\"/taxi.min_fare: \"2.50\"/' spark-pipeline.yml"

section database
x "sed -i '/^configuration:/,\$d' spark-pipeline.yml; echo 'database: taxi_dev' >> spark-pipeline.yml; cat spark-pipeline.yml"
x "spark-pipelines run $R 2>&1 | ev"

section envs
mkdir -p envs
cat > envs/dev.yml <<YML
name: taxi_etl
storage: file://$P/storage/dev
database: taxi_dev
libraries:
  - glob:
      include: ../transformations/**
configuration:
  spark.sql.shuffle.partitions: "4"
  taxi.min_fare: "0"
YML
sed -e 's/dev/prod/g' -e 's/"4"/"16"/' -e 's/min_fare: "0"/min_fare: "2.50"/' envs/dev.yml > envs/prod.yml
sed -i '/^database:/d' spark-pipeline.yml
x 'cat envs/dev.yml'
x 'diff envs/dev.yml envs/prod.yml'
x "python - $PORT <<'PY'
import sys
from pyspark.sql import SparkSession
s = SparkSession.builder.remote(f'sc://localhost:{sys.argv[1]}').getOrCreate()
for db in ['taxi_dev', 'taxi_prod']:
    s.sql(f'CREATE DATABASE IF NOT EXISTS {db}')
print([d.name for d in s.catalog.listDatabases()])
PY"
x "spark-pipelines run $R --spec envs/dev.yml 2>&1 | ev"
x "spark-pipelines run $R --spec envs/prod.yml 2>&1 | ev"
x "python - $PORT <<'PY'
import sys
from pyspark.sql import SparkSession
s = SparkSession.builder.remote(f'sc://localhost:{sys.argv[1]}').getOrCreate()
for db in ['taxi_dev', 'taxi_prod']:
    n = s.table(f'{db}.clean_trips').count()
    print(f'{db}.clean_trips: {n:,} rows   tables:', sorted(t.name for t in s.catalog.listTables(db)))
PY"
x 'ls storage storage/*/_checkpoints/spark_catalog'

section env-vars
x "sed -i 's/WHERE fare_amount > \${taxi.min_fare}/WHERE fare_amount > 0/' transformations/clean_trips.sql"
x "printf 'name: taxi_etl\nstorage: \${PIPELINE_STORAGE}\nlibraries:\n  - glob:\n      include: transformations/**\n' > /tmp/sdp-env.yml && cat /tmp/sdp-env.yml"
x "cp /tmp/sdp-env.yml ./spark-pipeline.env.yml"
x "PIPELINE_STORAGE=file://$P/storage/env spark-pipelines run $R --spec spark-pipeline.env.yml 2>&1 | grep -E 'Run is|Exception|Error' -A2 | cut -c1-160 | head -6"
x 'ls -d storage/env "\${PIPELINE_STORAGE}" 2>&1'
x 'rm -rf "\${PIPELINE_STORAGE}" spark-pipeline.env.yml'
x "PIPELINE_STORAGE=file://$P/storage/env envsubst < /tmp/sdp-env.yml > spark-pipeline.env.yml && grep storage spark-pipeline.env.yml"
x "TAXI_ENV=ci spark-pipelines run $R --spec spark-pipeline.env.yml 2>&1 | grep -E 'TAXI_ENV|Run is'"
x 'ls storage/env/_checkpoints/spark_catalog'
x 'rm spark-pipeline.env.yml /tmp/sdp-env.yml'
