#!/usr/bin/env bash
# Build the jplug Connect plugin jar with Maven against Spark 4.3.0 (Scala 2.13).
#   ch86/build.sh   ->  ch86/jplug/target/jplug-0.1.0.jar
set -euo pipefail
cd "$(dirname "$0")/jplug"
mvn -q -B package
echo "built $(ls target/jplug-*.jar) with $(mvn -v | head -1 | sed 's/\x1b\[[0-9;]*m//g'), $(javac -version 2>&1)"
