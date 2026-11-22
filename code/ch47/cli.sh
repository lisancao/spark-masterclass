#!/usr/bin/env bash
# Chapter 47: every spark-pipelines subcommand and flag, spec discovery, library globs and errors.
# Output: output/<section>.txt.  CONNECT_PORT=15104 SPARK_UI_PORT=4144 bash ch47/cli.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; OUT="$HERE/output"
. "$HERE/../common/sdp.sh"
sdp_venv
W="$BOOK_DATA/work/sdp/ch47"; rm -rf "$W"; mkdir -p "$W"; cd "$W"
R="--remote sc://localhost:$PORT"
section() { exec > >(clean > "$OUT/$1.txt") 2>&1; }
trap sdp_stop EXIT

section help
x 'spark-pipelines --help | head -3'
x 'spark-pipelines run --help'
x 'spark-pipelines dry-run --help'
x 'spark-pipelines init --help'

section init
x 'spark-pipelines init --name demo'
x 'spark-pipelines init --name demo 2>&1 | tail -1; echo "exit code: ${PIPESTATUS[0]}"'
x 'spark-pipelines init 2>&1 | tail -1'

# A project for the rest of the chapter, and a server whose current folder is the project.
sdp_project "$W/taxi_etl"
sdp_server "$W/taxi_etl" --conf spark.sql.catalogImplementation=hive
cd "$W/taxi_etl"

section discovery
x 'mkdir -p transformations/sub && cd transformations/sub'
x "spark-pipelines dry-run $R 2>&1 | grep -E 'Loading pipeline spec|Run is'"
x 'cd ../..'
x 'cd /tmp'
x "spark-pipelines dry-run $R 2>&1 | tail -1; echo \"exit code: \${PIPESTATUS[0]}\""
x 'cd -'
x 'cp spark-pipeline.yml spark-pipeline.yaml'
x "spark-pipelines dry-run $R 2>&1 | tail -1"
x 'rm spark-pipeline.yaml'
x "spark-pipelines dry-run $R --spec nothing/here.yml 2>&1 | tail -1"
x 'mkdir -p ../specs && cp spark-pipeline.yml ../specs/dev.yml'
x "spark-pipelines dry-run $R --spec ../specs/dev.yml 2>&1 | grep -E 'Loading pipeline spec|Root directory|Run is'"
x 'rm -r ../specs transformations/sub'

section globs
spec() { printf 'name: taxi_etl\nstorage: file://%s/pipeline-storage\nlibraries:\n%s\n' "$PWD" "$1" > spark-pipeline.yml; }
x 'cp spark-pipeline.yml /tmp/sdp-spec.bak'
spec '  - glob:
      include: transformations/**/*.sql'
x 'cat spark-pipeline.yml | tail -3'
x "spark-pipelines dry-run $R 2>&1 | tail -1"
spec '  - glob:
      include: transformations/raw.py
  - glob:
      include: transformations/clean_trips.sql'
x 'cat spark-pipeline.yml | tail -5'
x "spark-pipelines dry-run $R 2>&1 | grep -E 'Found|Run is|Exception' | cut -c1-200"
cp /tmp/sdp-spec.bak spark-pipeline.yml; rm /tmp/sdp-spec.bak
x 'echo "# Taxi pipeline" > transformations/README.md'
x "spark-pipelines dry-run $R 2>&1 | grep -E 'Found|Exception'"
x 'rm transformations/README.md'
x 'mkdir -p lib && printf "FARE_MIN = 0\n" > lib/rules.py'
x 'sed -i "s/^from pyspark import pipelines as dp/&\nfrom lib.rules import FARE_MIN/" transformations/raw.py && head -3 transformations/raw.py'
x "spark-pipelines dry-run $R 2>&1 | grep -E 'Error|Run is'"
x "PYTHONPATH=\$PWD spark-pipelines dry-run $R 2>&1 | grep -E 'Error|Run is'"
x 'sed -i "/from lib.rules/d" transformations/raw.py && rm -r lib'
x 'ls transformations; find . -name __pycache__'

section wrapper
x "spark-pipelines $R --class Foo run 2>&1 | grep -E 'ERROR|class'; echo \"exit code: \${PIPESTATUS[0]}\""
x "spark-pipelines $R --conf spark.api.mode=classic run 2>&1 | grep -E 'ERROR'; echo \"exit code: \${PIPESTATUS[0]}\""
x "spark-pipelines $R run --name nightly 2>&1 | tail -1; echo \"exit code: \${PIPESTATUS[0]}\""
x "spark-pipelines $R run --refresh 2>&1 | tail -1"
x "spark-pipelines $R --verbose dry-run 2>&1 | grep -E '^(Main class|Arguments|  |--remote|Spark config)' | head -14"

section run-first
x "spark-pipelines run $R 2>&1 | grep -E 'Run is'; echo \"exit code: \${PIPESTATUS[0]}\""
x 'find pipeline-storage -maxdepth 5 -type d | sort'

section refresh
q() { python - "$PORT" <<'PY'
import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote(f"sc://localhost:{sys.argv[1]}").getOrCreate()
for t in ["raw_trips", "clean_trips", "zones", "borough_daily"]:
    print(f"{t:14} {spark.table(t).count():>9,} rows")
PY
}
ev() { grep -E 'is (QUEUED|EXCLUDED)|COMPLETED|FAILED|Exception' | sed -E 's/^[0-9-]+ //; s/spark_catalog.default.//g' | cut -c1-120; }
x "spark-pipelines run $R --refresh borough_daily 2>&1 | ev"
x "spark-pipelines run $R --refresh zones,borough_daily 2>&1 | ev"
x "spark-pipelines run $R --full-refresh raw_trips 2>&1 | ev"
x 'q'
x 'ls pipeline-storage/_checkpoints/spark_catalog/default/raw_trips/raw_trips'
x "spark-pipelines run $R --full-refresh clean_trips --refresh borough_daily 2>&1 | ev"
x 'q'
x "spark-pipelines run $R --full-refresh-all 2>&1 | ev"
x 'q'
x 'ls pipeline-storage/_checkpoints/spark_catalog/default/*/*'
x "spark-pipelines run $R --refresh no_such_table 2>&1 | tail -2 | cut -c1-200"
x "spark-pipelines run $R --refresh zones --full-refresh zones 2>&1 | tail -2 | cut -c1-200"
x "spark-pipelines run $R --full-refresh-all --full-refresh zones 2>&1 | tail -3"

section dry-run
x 'mv pipeline-storage pipeline-storage.keep; mv spark-warehouse spark-warehouse.keep'
x "sed -i 's/avg(c.fare_amount)/avg(c.fare)/' transformations/borough_daily.sql"
x "spark-pipelines dry-run $R 2>&1 | grep -E 'Failed to resolve flow:|Error:' | cut -c1-150; echo \"exit code: \${PIPESTATUS[0]}\""
x "sed -i 's/avg(c.fare)/avg(c.fare_amount)/' transformations/borough_daily.sql"
x "spark-pipelines dry-run $R 2>&1 | tail -1; echo \"exit code: \${PIPESTATUS[0]}\""
x 'ls -d pipeline-storage spark-warehouse 2>&1'
x 'rm -rf pipeline-storage spark-warehouse; mv pipeline-storage.keep pipeline-storage; mv spark-warehouse.keep spark-warehouse'

section exit-codes
x "spark-pipelines run $R > /dev/null 2>&1; echo \"run ok: \$?\""
x "printf 'name: x\n' > /tmp/bad.yml; spark-pipelines run $R --spec /tmp/bad.yml > /dev/null 2>&1; echo \"spec error: \$?\"; rm /tmp/bad.yml"
x "spark-pipelines run --remote sc://localhost:1 > /dev/null 2>&1 & p=\$!; sleep 20; kill -0 \$p 2>/dev/null && echo 'no server: still waiting after 20 s' && pkill -P \$p; kill \$p 2>/dev/null; wait \$p 2>/dev/null"

section remote
sdp_stop
x 'mkdir -p /tmp/sdp-elsewhere'
sdp_server /tmp/sdp-elsewhere --conf spark.sql.catalogImplementation=hive
x "spark-pipelines run $R 2>&1 | grep -E 'Failed to resolve|Error:|Path does not exist|Run is' | cut -c1-160 | head -6; echo \"exit code: \${PIPESTATUS[0]}\""
x 'ls /tmp/sdp-elsewhere'
sdp_stop
rm -rf /tmp/sdp-elsewhere
