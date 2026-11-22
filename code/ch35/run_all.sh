#!/usr/bin/env bash
# Chapter 35: regenerate every output file. Run from code/:  ch35/run_all.sh
set -euo pipefail
cd "$(dirname "$0")/.."
for s in prepare two_errors define grains rules lifecycle; do
  .venv/bin/python ch35/$s.py 2>/dev/null | sed "s#$HOME#~#g" > ch35/output/$s.txt
done
