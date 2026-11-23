#!/usr/bin/env bash
# Chapter 24: the size of a full pyspark install and of pyspark-client, and a query from the thin one.
#   ch24/sizes.sh <scratch dir>
set -euo pipefail
cd "$(dirname "$0")/.."
S=$1; rm -rf "$S/thin"
uv venv -q -p 3.12 "$S/thin" && VIRTUAL_ENV=$S/thin uv pip install -q pyspark-client==4.3.0
full=$(uv run python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
thin=$S/thin/lib/python3.12/site-packages/pyspark
echo "pyspark 4.3.0 (full):   pyspark/ $(du -sm "$full" | cut -f1) MB, jars/ $(du -sm "$full/jars" | cut -f1) MB in $(ls "$full/jars" | wc -l) files"
echo "pyspark-client 4.3.0:   pyspark/ $(du -sm "$thin" | cut -f1) MB, jars/ none"
echo "pyspark-client venv, all dependencies: $(du -sm "$S/thin" | cut -f1) MB"
"$S/thin/bin/python" ch24/thin_client.py 2>/dev/null
