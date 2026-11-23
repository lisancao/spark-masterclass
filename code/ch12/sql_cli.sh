#!/usr/bin/env bash
# Chapter 12: the spark-sql command-line shell, run two times against one metastore.
# The CLI always uses the Hive catalog. Here its metastore is a Derby database in the work
# folder, so the second run sees the table that the first run made.
# Run from code/:  ch12/sql_cli.sh > ch12/output/sql_cli.txt
set -euo pipefail
cd "$(dirname "$0")/.."
W="${BOOK_DATA:-$PWD/../data}/work/ch12-cli"
[[ ${KEEP:-0} == 1 ]] || rm -rf "$W"
mkdir -p "$W"
export SPARK_HOME=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOCAL_IP=127.0.0.1
ZONES="${BOOK_DATA:-$PWD/../data}/nyc-taxi/taxi_zone_lookup.csv"
CONF=(--master "local[2]" --conf spark.ui.port=${SPARK_UI_PORT:-4142}
      --conf "spark.sql.warehouse.dir=$W/warehouse"
      --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$W/metastore_db;create=true"
      --driver-java-options "-Dderby.system.home=$W")
sql() {  # run spark-sql, keep the result lines and the timing lines
  (cd "$W" && "$SPARK_HOME/bin/spark-sql" "${CONF[@]}" "$@" 2>&1) \
    | grep -v -E 'WARN|^Using Spark|^Setting default|^To adjust|^Spark Web UI|^Spark master|^WARNING|^\s*$|^[0-9/]+ [0-9:]+ (INFO|ERROR)' \
    | sed "s#$HOME#~#g"
}
echo "== 1. spark-sql -e: one shell, a few statements"
echo "\$ spark-sql -e \"SHOW CATALOGS; CREATE DATABASE taxi; ...\""
sql -e "
SHOW CATALOGS;
SELECT current_catalog(), current_database();
CREATE DATABASE taxi;
CREATE TABLE taxi.zones USING csv OPTIONS (path '$ZONES', header 'true', inferSchema 'true');
SELECT Borough, count(*) AS zones FROM taxi.zones GROUP BY Borough ORDER BY zones DESC;"
echo
echo "== 2. a second spark-sql process: the table is still in the metastore"
echo "\$ spark-sql --database taxi -e \"SHOW TABLES; DESCRIBE TABLE EXTENDED zones\""
sql --database taxi -e "SHOW TABLES; DESCRIBE TABLE EXTENDED zones;" | grep -E -v '^(LocationID|Borough|Zone|service_zone|# |Created|Last|Serde|InputFormat|OutputFormat|Storage)' 
echo
echo "== 3. spark-sql -f: a file of statements, with a variable from --hivevar"
cat > "$W/top_zones.sql" <<'SQL'
-- top_zones.sql
SELECT Zone FROM taxi.zones WHERE Borough = '${borough}' ORDER BY Zone LIMIT 3;
SQL
echo "\$ spark-sql --hivevar borough=Bronx -f top_zones.sql"
sql --hivevar borough=Bronx -f top_zones.sql
echo
echo "== 4. what is on disk"
(cd "$W" && find . -maxdepth 3 -not -path './metastore_db/*' -not -name '*.crc' | sort | sed 's/^/  /')
