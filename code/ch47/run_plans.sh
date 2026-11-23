#!/usr/bin/env bash
# Chapter 47: save every plan and fallback report the chapter shows (no timings, no lock).
# Run from code/:  BOOK_DATA=~/spark-masterclass/data SPARK_UI_PORT=4062 bash ch47/run_plans.sh
set -uo pipefail
export SPARK_LOCAL_IP=${SPARK_LOCAL_IP:-127.0.0.1}
O=ch47/output; V=$BOOK_DATA/ch47
py() { case $1 in gluten) echo "$V/venv-4.1.1/bin/python";;
                  comet|rapids|auron) echo "$V/venv-4.2.0/bin/python";; *) echo .venv/bin/python;; esac; }
clean() { sed -e 's/.*\r//' -e "s#$HOME#~#g" | grep -v -e '^\[Stage' -e '^\s*$'; }

.venv/bin/python ch47/columnar_43.py 2>/dev/null | clean > $O/columnar_43.txt
CH24_LOG=WARN .venv/bin/python ch47/blocked_43.py 2>/dev/null | clean > $O/blocked_43.txt
for e in cpu gluten comet rapids auron; do
  QUERY_TIMEOUT=300 "$(py $e)" ch47/plans.py $e 2>/dev/null | clean > $O/plans-$e.txt
done
# Spark's defaults (ANSI on, TIMESTAMP_NTZ inference on), with each engine's fallback log.
for e in gluten comet rapids auron; do
  CH24_LOG=WARN "$(py $e)" ch47/plans.py $e --defaults hourly boroughs \
      > $O/plans-default-$e.txt 2> $O/log-default-$e.txt
  clean < $O/plans-default-$e.txt > $O/tmp && mv $O/tmp $O/plans-default-$e.txt
done
grep -o 'Validation failed for plan: .*' $O/log-default-gluten.txt | sed 's/\[QueryId=[0-9]*\]//' \
  | sort | uniq -c | sort -rn | clean > $O/fallbacks-gluten.txt
grep -E '^\s*[!@]' $O/log-default-rapids.txt | sed 's/#[0-9]*//g' | awk '!seen[$0]++' \
  | clean > $O/fallbacks-rapids.txt
grep -E 'COMET:|Comet accelerated' $O/log-default-comet.txt | sed 's/#[0-9]*//g' \
  | sed 's/^[ +:-]*//' | awk '!seen[$0]++' | clean > $O/fallbacks-comet.txt
grep -o 'Falling back exec: .*' $O/log-default-auron.txt \
  | sed 's/tableIdentifier: unknown, class: .*/(CSV scan)/' | sort | uniq -c | sort -rn \
  | clean > $O/fallbacks-auron.txt
rm -f $O/log-default-*.txt
# The ANSI-off step for Gluten alone: TIMESTAMP_NTZ is the next thing it cannot take.
CH24_LOG=WARN "$(py gluten)" ch47/plans.py gluten --defaults --conf=spark.sql.ansi.enabled=false \
    hourly 2> $O/log.txt | clean > $O/plans-gluten-ansi-off.txt
grep -o 'Validation failed for plan: .*' $O/log.txt | sed 's/\[QueryId=[0-9]*\]//' \
  | sort | uniq -c | sort -rn | clean > $O/fallbacks-gluten-ansi-off.txt
rm -f $O/log.txt
# Comet, with its CSV reader switched on: the build side of the join comes into Comet.
CH24_LOG=WARN "$(py comet)" ch47/plans.py comet --conf=spark.comet.convert.csv.enabled=true \
    boroughs 2>/dev/null | clean > $O/plans-comet-csv.txt
