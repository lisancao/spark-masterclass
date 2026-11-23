#!/usr/bin/env bash
# Build the jext extension jar with Maven against Spark 4.3.0 (Scala 2.13).
#   ch85/build.sh   ->  ch85/jext/target/jext-0.1.0.jar
set -euo pipefail
cd "$(dirname "$0")/jext"
mvn -q -B package
echo "built $(ls target/jext-*.jar) with $(mvn -v | head -1 | sed 's/\x1b\[[0-9;]*m//g'), $(javac -version 2>&1)"
