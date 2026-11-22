#!/usr/bin/env bash
# Chapter 17: regenerate every output file. Run from code/:  ch17/run_all.sh
set -euo pipefail
cd "$(dirname "$0")/.."
for s in prepare two_errors define grains rules lifecycle; do
  .venv/bin/python ch17/$s.py 2>/dev/null | sed "s#$HOME#~#g" > ch17/output/$s.txt
done
