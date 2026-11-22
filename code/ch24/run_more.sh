#!/usr/bin/env bash
# Chapter 24: the plugin, memory and Comet UDF outputs (no timings, no lock).
# Run from code/:  BOOK_DATA=~/spark-masterclass/data SPARK_UI_PORT=4062 bash ch24/run_more.sh
set -uo pipefail
export SPARK_LOCAL_IP=${SPARK_LOCAL_IP:-127.0.0.1}
O=ch24/output; V=$BOOK_DATA/ch24
P41=$V/venv-4.1.1/bin/python; P42=$V/venv-4.2.0/bin/python
clean() { sed -e 's/.*\r//' -e "s#$HOME#~#g" | grep -v -e '^\[Stage' -e '^\s*$'; }
{ $P41 ch24/inject.py gluten; $P42 ch24/inject.py comet; $P42 ch24/inject.py rapids; $P42 ch24/inject.py auron; } \
  2>/dev/null | clean > $O/inject.txt
{ $P41 ch24/memory.py gluten-no-offheap; $P42 ch24/memory.py rapids; } 2>/dev/null | clean > $O/memory.txt
$P42 ch24/repro_comet_udf.py 2>/dev/null | clean > $O/repro_comet_udf.txt
QUERY_TIMEOUT=300 $P42 ch24/plans.py comet 2>/dev/null | clean > $O/plans-comet.txt
CH24_LOG=WARN $P42 ch24/plans.py comet --conf=spark.comet.convert.csv.enabled=true \
    boroughs 2>/dev/null | clean > $O/plans-comet-csv.txt
$P42 ch24/plans.py comet --conf=spark.sql.execution.arrow.pythonUDF.columnarInput.enabled=false \
    udf 2>/dev/null | clean > $O/plans-comet-udf-rowinput.txt
# Why bench.py builds a new DataFrame for each run, and the one row that differs on the GPU.
.venv/bin/python ch24/reuse.py 2>/dev/null | clean > $O/reuse.txt
{ $P42 ch24/compare_rows.py cpu boroughs; $P42 ch24/compare_rows.py rapids boroughs; } \
  2>/dev/null | clean > $O/compare-boroughs.txt
