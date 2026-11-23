#!/usr/bin/env bash
# Chapter 17: read the Derby metastore that ch17/partitions.py left behind, with Derby's own ij tool.
# Run from code/ after partitions.py has stopped (Derby accepts one process):
#   ch17/derby_rows.sh > ch17/output/derby_rows.txt
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
db=$(cd "$here/../.." && pwd)/data/work/ch17-partitions/metastore_db
jars=$(cd "$here/.." && .venv/bin/python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__) + '/jars')")
cp=$(ls "$jars"/derby*.jar | tr '\n' ':')
sql=$(mktemp)
cat > "$sql" <<SQL
connect 'jdbc:derby:$db';
-- 1. databases and tables
SELECT CAST(d.NAME AS VARCHAR(8)) AS db, CAST(t.TBL_NAME AS VARCHAR(10)) AS tbl, CAST(t.TBL_TYPE AS VARCHAR(14)) AS type
  FROM DBS d JOIN TBLS t ON d.DB_ID = t.DB_ID ORDER BY 1, 2;
-- 2. the storage descriptor and the SerDe of each table
SELECT CAST(t.TBL_NAME AS VARCHAR(10)) AS tbl, CAST(s.INPUT_FORMAT AS VARCHAR(64)) AS input_format,
       CAST(SUBSTR(se.SLIB, LOCATE('serde', se.SLIB)) AS VARCHAR(36)) AS serde
  FROM TBLS t JOIN SDS s ON t.SD_ID = s.SD_ID JOIN SERDES se ON s.SERDE_ID = se.SERDE_ID ORDER BY 1;
-- 3. one row per partition, each with its own storage descriptor and location
SELECT CAST(t.TBL_NAME AS VARCHAR(10)) AS tbl, CAST(p.PART_NAME AS VARCHAR(18)) AS part_name,
       CAST(SUBSTR(s.LOCATION, LOCATE('taxi.db', s.LOCATION)) AS VARCHAR(40)) AS location
  FROM PARTITIONS p JOIN TBLS t ON p.TBL_ID = t.TBL_ID JOIN SDS s ON p.SD_ID = s.SD_ID ORDER BY 1, 2;
-- 4. what Spark adds for a data source table: its provider and schema, as table parameters
SELECT CAST(tp.PARAM_KEY AS VARCHAR(40)) AS param_key, CAST(SUBSTR(CAST(tp.PARAM_VALUE AS VARCHAR(4000)), 1, 44) AS VARCHAR(44)) AS param_value
  FROM TABLE_PARAMS tp JOIN TBLS t ON tp.TBL_ID = t.TBL_ID
  WHERE t.TBL_NAME = 'zones_ds' AND tp.PARAM_KEY LIKE 'spark.sql.%' ORDER BY 1;
SQL
java -Dij.maximumDisplayWidth=120 -cp "$cp" org.apache.derby.tools.ij "$sql" 2>&1 | grep -v -E "^ij version|^ij> connect|^$" | sed 's/^ij> //'
rm -f "$sql"
