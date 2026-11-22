#!/usr/bin/env bash
# Chapter 33: regenerate every output file. Run from code/:  ch33/run_all.sh
set -euo pipefail
cd "$(dirname "$0")/.."
for s in prepare two_errors define grains rules lifecycle; do
  .venv/bin/python ch33/$s.py 2>/dev/null | sed "s#$HOME#~#g" > ch33/output/$s.txt
done
