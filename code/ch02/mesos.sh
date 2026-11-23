#!/usr/bin/env bash
# Chapter 2: what spark-submit 4.3.0 says to a mesos:// master URL (Mesos was removed in 4.0).
#   ch02/mesos.sh        (from code/; uses the tarball of Chapter 6 through ch70/env.sh)
cd "$(dirname "$0")/.."
source ch70/env.sh
"$SPARK_HOME/bin/spark-submit" --master mesos://127.0.0.1:5050 ch71/probe.py x 2>&1 | grep -v incubator | head -3
