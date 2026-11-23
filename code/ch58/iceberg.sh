#!/usr/bin/env bash
# Chapter 58: the taxi pipeline in an Iceberg catalog. The Iceberg runtime for Spark 4.3 is an
# unreleased build (see notes/ch55.md). Output: output/iceberg-*.txt, output/static-warning.txt
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; OUT="$HERE/output"
. "$HERE/../common/sdp.sh"
sdp_venv
JAR="$BOOK_DATA/jars/iceberg-spark-runtime-4.3_2.13-b1eab64.jar"
W="$BOOK_DATA/work/sdp/ch79i"; rm -rf "$W"; mkdir -p "$W"
R="--remote sc://localhost:$PORT"
section() { exec > >(clean > "$OUT/$1.txt") 2>&1; }
trap sdp_stop EXIT
sdp_project "$W/taxi_etl"; P="$W/taxi_etl"; cd "$P"
py() { python - "$PORT" "$@"; }

section iceberg-server
LAKE=(--jars "$JAR"
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.lake.type=hadoop
  --conf "spark.sql.catalog.lake.warehouse=file://$P/lake")
x 'echo "server options: ${LAKE[*]}" | sed "s/ --/\n  --/g"'
sdp_server "$P" "${LAKE[@]}"
x "printf 'catalog: lake\ndatabase: taxi\n' >> spark-pipeline.yml && cat spark-pipeline.yml"
x "spark-pipelines run $R 2>&1 | grep -E 'Exception|Run is' | cut -c1-170"
x "py <<'PY'
import sys
from pyspark.sql import SparkSession
s = SparkSession.builder.remote(f'sc://localhost:{sys.argv[1]}').getOrCreate()
s.sql('CREATE NAMESPACE IF NOT EXISTS lake.taxi')
print('namespaces:', [r[0] for r in s.sql('SHOW NAMESPACES IN lake').collect()])
PY"
x "spark-pipelines run $R 2>&1 | grep -E 'Flow .* is QUEUED|Exception|Run is' | sed -E 's/^[0-9-]+ //' | cut -c1-170"
x "py <<'PY'
import sys
from pyspark.sql import SparkSession
s = SparkSession.builder.remote(f'sc://localhost:{sys.argv[1]}').getOrCreate()
for t in ['raw_trips', 'clean_trips', 'zones', 'borough_daily']:
    n = s.table(f'lake.taxi.{t}').count()
    snaps = s.sql(f'SELECT operation FROM lake.taxi.{t}.snapshots ORDER BY committed_at').collect()
    print(f'lake.taxi.{t:14} {n:>8,} rows   snapshots: {[r[0] for r in snaps]}')
PY"
x 'find lake -maxdepth 3 -type d | sort'
x 'ls spark-warehouse 2>&1 | head -2'
x "python \"$SDP_CODE/ch56/make_days.py\" data/trips 2026-06-03"
x "spark-pipelines run $R 2>&1 | grep -E 'Run is'"
x "py <<'PY'
import sys
from pyspark.sql import SparkSession
s = SparkSession.builder.remote(f'sc://localhost:{sys.argv[1]}').getOrCreate()
for t in ['raw_trips', 'borough_daily']:
    snaps = s.sql(f'SELECT operation, summary[\"added-records\"] AS added FROM lake.taxi.{t}.snapshots ORDER BY committed_at').collect()
    print(f'lake.taxi.{t:14}', [(r[0], r[1]) for r in snaps])
PY"
sdp_stop

section iceberg-spec-conf
rm -rf lake pipeline-storage metastore_db spark-warehouse
sdp_server "$P" --jars "$JAR"
cat > spark-pipeline.yml <<YML
name: taxi_etl
storage: file://$P/pipeline-storage
catalog: lake
database: taxi
libraries:
  - glob:
      include: transformations/**
configuration:
  spark.sql.catalog.lake: org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.lake.type: hadoop
  spark.sql.catalog.lake.warehouse: file://$P/lake
YML
x 'cat spark-pipeline.yml'
x "spark-pipelines run $R 2>&1 | grep -E 'Exception|Run is' | cut -c1-170"
x "py \"$P\" <<'PY'
import sys
from pyspark.sql import SparkSession
s = SparkSession.builder.remote(f'sc://localhost:{sys.argv[1]}').getOrCreate()
print('server default:', s.conf.get('spark.sql.catalog.lake', '<not set>'))
s.conf.set('spark.sql.catalog.lake', 'org.apache.iceberg.spark.SparkCatalog')
s.conf.set('spark.sql.catalog.lake.type', 'hadoop')
s.conf.set('spark.sql.catalog.lake.warehouse', f'file://{sys.argv[2]}/lake')
s.sql('CREATE NAMESPACE IF NOT EXISTS lake.taxi')
PY"
x "spark-pipelines run $R 2>&1 | grep -E 'Flow .* is QUEUED|Exception|Run is' | sed -E 's/^[0-9-]+ //' | cut -c1-170"
x 'find lake -maxdepth 2 -type d | sort'

