#!/usr/bin/env bash
# Chapter 34: a dry run of the crm pipeline with sequence_by set to a column that does not exist.
# Run from code/ after ch34/drops.sh:  ch34/dry_seq.sh > ch34/output/dry_seq.txt
set -uo pipefail
cd "$(dirname "$0")/.."
W="${BOOK_DATA:-../data}/work/part06/ch34-crm"
sed -i 's/sequence_by="seq",/sequence_by="sequence",/' "$W/transformations/customers.py"
echo '$ grep -n sequence_by transformations/customers.py'
grep -n 'sequence_by' "$W/transformations/customers.py"
ICEBERG=1 common/pipeline.sh ch34/crm part06/ch34-crm dry-run \
  | grep -E 'Failed to resolve flow|Exception: ' | grep -v upstream | sed -E 's/^[0-9-]+ //; s/^.*(pyspark\.errors[a-z.]+Exception)/\1/' | cut -c1-200
sed -i 's/sequence_by="sequence",/sequence_by="seq",/' "$W/transformations/customers.py"
