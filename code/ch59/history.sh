#!/usr/bin/env bash
# Chapter 59, Step 6: which releases have the metric view read path, and a trap in a shallow clone.
#   Run from code/:  SPARK_SRC=<a clone of apache/spark with its tags> ch59/history.sh
set -uo pipefail
g() { git -C "$SPARK_SRC" "$@" 2>/dev/null; }
f=sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/SessionCatalog.scala
echo "lines that mention MetricView in SessionCatalog.scala, per release tag:"
for t in v4.0.0 v4.1.0 v4.2.0 v4.3.0; do
  printf '  %-7s %s\n' "$t" "$(g show "$t:$f" | grep -c MetricView)"
done
echo
echo "is the clone shallow? $(g rev-parse --is-shallow-repository)"
echo "the oldest commit it has: $(g log -1 --format='%h %ad %s' --date=short "$(g rev-list --max-parents=0 HEAD | head -1)")"
echo
echo "git log -S 'parseMetricViewDefinition' v4.3.0 (oldest first):"
g log --reverse --format='  %h %ad %s' --date=short -S'parseMetricViewDefinition' v4.3.0 -- "$f" | head -3
echo
echo "commits between v4.1.0 and v4.2.0 that name metric views:"
g log --format='  %h %s' v4.1.0..v4.2.0 -i --grep='metric view' | tail -4
