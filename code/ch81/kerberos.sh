#!/usr/bin/env bash
# Chapter 81, Step 7: what spark-submit checks for Kerberos without a KDC. There is no KDC in the
# book's setup, so this only shows the checks that run before any login.
#   ch81/kerberos.sh        (from code/)
set -uo pipefail
cd "$(dirname "$0")"
export BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data} SPARK_LOCAL_IP=127.0.0.1
SUBMIT=$BOOK_DATA/tarball/spark-4.3.0-bin-hadoop3/bin/spark-submit
export PYSPARK_PYTHON=$(cd .. && pwd)/.venv/bin/python
first() { grep -m1 -E "Exception|Error:|error|ok," | sed -E 's/^Exception in thread "main" //' | cut -c1-110; }
echo "--- --principal without --keytab"
$SUBMIT --master "local[1]" --principal analyst@BOOK.EXAMPLE probe.py k 2>&1 | first
echo "--- a keytab file that does not exist"
$SUBMIT --master "local[1]" --principal analyst@BOOK.EXAMPLE --keytab /nonexistent.keytab probe.py k 2>&1 | first
echo "--- --proxy-user together with --principal"
touch /tmp/ch81-empty.keytab
$SUBMIT --master "local[1]" --proxy-user bob --principal analyst@BOOK.EXAMPLE --keytab /tmp/ch81-empty.keytab probe.py k 2>&1 | first
rm -f /tmp/ch81-empty.keytab
echo "--- no Kerberos at all: Hadoop security is off, no tokens"
$SUBMIT --master "local[1]" --conf spark.ui.port=4151 probe.py ch81-krb 2>&1 | grep -E "ok,|HadoopDelegationTokenManager|security.UserGroupInformation" | head -3
