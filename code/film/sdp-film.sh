# Hidden setup for the SDP part films (chapters 77 to 79). Source it from bash in a tape.
#   sdp_film_prepare <name>     a fresh taxi_etl copy with day 1, a server started in it, cd there
#   sdp_film_end                stop the server
set +e
. "$FILM_DEMO/common/sdp.sh"
sdp_venv >/dev/null 2>&1
export R="--remote sc://localhost:$PORT"
sdp_film_prepare() {
  local d="$BOOK_DATA/work/sdp/film-$1"
  rm -rf "$d"; mkdir -p "$d/data" "$d/transformations"
  cp "$FILM_DEMO/ch56/project/transformations/"* "$d/transformations/"
  cp "$BOOK_DATA/nyc-taxi/taxi_zone_lookup.csv" "$d/data/"
  cp "$FILM_DEMO/ch56/make_days.py" "$FILM_DEMO/ch56/look.py" "$d/"
  python "$d/make_days.py" "$d/data/trips" 2026-06-01 >/dev/null
  printf 'name: taxi_etl\nstorage: file://%s/pipeline-storage\nlibraries:\n  - glob:\n      include: transformations/**\n' "$d" > "$d/spark-pipeline.yml"
  sdp_server "$d" --conf spark.sql.catalogImplementation=hive >/dev/null 2>&1
  cd "$d"
}
sdp_film_end() { sdp_stop >/dev/null 2>&1; }
# short views of a run for a 95-column terminal
ev() { grep --line-buffered -E 'Flow .*(RUNNING|COMPLETED|FAILED)|Run is|Exception|^probe' | sed -u -E 's/^[0-9-]+ //; s/spark_catalog\.default\.//g' | cut -c1-92; }
counts() { python look.py "sc://localhost:$PORT" 2>/dev/null | grep rows; }
short() { sed -u "s#$HOME#~#g; s#$BOOK_DATA/work/sdp/##g" | cut -c1-88; }
sdp_film_ch79_setup() {
  cp "$FILM_DEMO/ch58/probe/probe.py" transformations/
  mkdir -p envs
  for e in dev prod; do
    printf 'name: taxi_etl\nstorage: file://%s/storage/%s\ndatabase: taxi_%s\nlibraries:\n  - glob:\n      include: ../transformations/**\n' "$PWD" $e $e > envs/$e.yml
  done
  printf 'configuration:\n  spark.sql.shuffle.partitions: "4"\n' >> envs/dev.yml
  printf 'configuration:\n  spark.sql.shuffle.partitions: "16"\n' >> envs/prod.yml
  python - "$PORT" >/dev/null 2>&1 <<'PY'
import sys
from pyspark.sql import SparkSession
s = SparkSession.builder.remote(f"sc://localhost:{sys.argv[1]}").getOrCreate()
for d in ["taxi_dev", "taxi_prod"]:
    s.sql(f"CREATE DATABASE IF NOT EXISTS {d}")
PY
  cp spark-pipeline.yml spark-pipeline.static.yml
  printf 'configuration:\n  spark.sql.catalogImplementation: "in-memory"\n' >> spark-pipeline.static.yml
}
show_probe() { grep --line-buffered -E 'shuffle|database|Run is' | cut -c1-80; }
count_errors() { grep -o 'CANNOT_MODIFY_STATIC_CONFIG' | uniq -c; }
