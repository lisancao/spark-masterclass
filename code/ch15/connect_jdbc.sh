#!/usr/bin/env bash
# Chapter 15: run ConnectJdbc.java against a Connect server (the one from ch08/connect_server.sh).
# Classpath: the JDBC driver and the JVM Connect client (jars/connect-repl/ of the tarball),
# then the tarball's jars/ for their dependencies.
# Run from code/:  ch15/connect_jdbc.sh > ch15/output/connect_jdbc.txt
set -euo pipefail
cd "$(dirname "$0")/.."
T=${SPARK_TARBALL:-${BOOK_DATA:-$PWD/../data}/tarball/spark-4.3.0-bin-hadoop3}
J=$T/jars
CP="$J/connect-repl/spark-connect-client-jdbc_2.13-4.3.0.jar:$J/connect-repl/spark-connect-client-jvm_2.13-4.3.0.jar"
# the driver needs Spark's common jars (spark-common-utils, network-common, scala-library, ...):
# take them from the same install. A Maven or sbt build resolves them from the driver's POM.
CP="$CP:$J/*"
ch08/connect_server.sh start
trap 'ch08/connect_server.sh stop' EXIT
echo "\$ java --add-opens=java.base/java.nio=ALL-UNNAMED -cp <driver jars> ConnectJdbc.java jdbc:sc://localhost:${CONNECT_PORT:-15102}"
java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "$CP" ch15/ConnectJdbc.java \
  "jdbc:sc://localhost:${CONNECT_PORT:-15102}" 2>&1 | grep -v -E "SLF4J|WARNING| INFO | WARN |^Using Spark"
