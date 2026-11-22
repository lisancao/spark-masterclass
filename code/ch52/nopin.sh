#!/usr/bin/env bash
# Chapter 52: build jdocs with the maven-compiler-plugin block removed from pom.xml, to see why it is pinned.
#   ch52/nopin.sh   (from code/)
set -uo pipefail
work=$(mktemp -d)
cp -r "$(dirname "$0")/jdocs/src" "$work/"
python3 -c "import re,sys; s=open(sys.argv[1]).read(); print(re.sub(r'<build>.*</build>','',s,flags=re.S))" \
  "$(dirname "$0")/jdocs/pom.xml" > "$work/pom.xml"
cd "$work"
echo "$(mvn -v | head -1 | sed 's/\x1b\[[0-9;]*m//g')"
mvn -B -q package 2>&1 | grep -E "Source option|maven-compiler-plugin:[0-9.]+:compile" | sed -E 's/ on project.*//' | sort -u
rm -rf "$work"
