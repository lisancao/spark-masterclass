#!/usr/bin/env bash
# Build the jdocs connector jar with Maven against Spark 4.3.0 (Scala 2.13).
#   ch55/build.sh   ->  ch55/jdocs/target/jdocs-0.1.0.jar
set -euo pipefail
cd "$(dirname "$0")/jdocs"
mvn -q -B package
echo "built $(ls target/jdocs-*.jar) with $(mvn -v | head -1 | sed 's/\x1b\[[0-9;]*m//g'), $(javac -version 2>&1)"
