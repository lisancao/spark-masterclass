#!/usr/bin/env bash
# Chapter 82 build: the v4.3.0 tag in a JDK 17 container, hadoop-provided + kubernetes.
set -euo pipefail
S=$(cd "$(dirname "$0")" && pwd)
start=$(date +%s)
docker run --rm --name ops-spark-build --user "$(id -u):$(id -g)" \
  -e HOME=/work -e MAVEN_OPTS="-Xss64m -Xmx4g -XX:ReservedCodeCacheSize=1g" \
  -v "$S/src:/work/spark" -v "$S/m2:/work/.m2" -w /work/spark \
  ops80/spark-build:jdk17 \
  ./dev/make-distribution.sh --name book --tgz -Phadoop-provided -Pkubernetes -DskipTests -Dmaven.repo.local=/work/.m2/repository
echo "BUILD SECONDS $(( $(date +%s) - start ))"
