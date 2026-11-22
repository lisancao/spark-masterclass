#!/usr/bin/env bash
# How to add the Go client to a module: the release tag does not resolve, the commit does.
set -uo pipefail
S=${SCRATCH:-/tmp/ch82-build}; mkdir -p "$S/gopath"
docker run --rm -u "$(id -u):$(id -g)" -e GOPATH=/gopath -e GOCACHE=/tmp/gocache -e HOME=/tmp -e GOFLAGS=-modcacherw \
  -v "$S/gopath:/gopath" golang:latest sh -c '
  cd /tmp && mkdir app && cd app && go mod init example.com/app >/dev/null 2>&1
  go version
  echo "$ go get github.com/apache/spark-connect-go/v40@v0.1.0"
  go get github.com/apache/spark-connect-go/v40@v0.1.0 2>&1 | tail -1
  echo "$ go get github.com/apache/spark-connect-go/v40@defb8525"
  go get github.com/apache/spark-connect-go/v40@defb8525 2>&1 | grep -i "added github.com/apache"
  grep spark-connect go.mod'
