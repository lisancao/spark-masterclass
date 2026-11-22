#!/usr/bin/env bash
# Run one step of Chapter 23 in the environment without pyspark:  ch23/step.sh step2_execute
set -euo pipefail
cd "$(dirname "$0")"
[[ -d build/gen ]] || ./gen.sh >/dev/null
uv run -q --isolated --no-project --with-requirements requirements.txt python "$1.py" 2>&1 | sed "s#$HOME#~#g"
