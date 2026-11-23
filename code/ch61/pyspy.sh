#!/usr/bin/env bash
# Chapter 61, Step 12: py-spy samples the driver and every Python worker it starts.
# py-spy starts the driver itself, so the kernel lets it read the workers (they are its
# descendants) even with kernel.yama.ptrace_scope=1.
#   ch61/pyspy.sh      (from code/)
set -euo pipefail
cd "$(dirname "$0")/.."
out=$(mktemp -d)
echo "ptrace_scope: $(cat /proc/sys/kernel/yama/ptrace_scope)"
SPARK_LOCAL_IP=127.0.0.1 py-spy record --subprocesses --format raw --rate 100 -o "$out/stacks.txt" \
  -- .venv/bin/python ch61/pyspy_app.py >/dev/null 2>&1
.venv/bin/python - "$out/stacks.txt" <<'PY'
import collections, re, sys
procs, leaves = collections.Counter(), collections.Counter()
for line in open(sys.argv[1]):
    stack, n = line.rsplit(" ", 1)
    frames = stack.split(";")
    chain = [re.sub(r"process \d+:", "", f).strip('"') for f in frames if re.match(r"process \d+:", f)]
    cmd = chain[-1]                                   # the process that the sample came from
    procs[cmd] += int(n)
    if "pyspark.daemon" in cmd and len(chain) > 3:    # a worker that the daemon forked
        leaf = re.sub(r"\S*/(site-packages|lib/python3\.\d+)/", "", frames[-1])
        leaves[leaf] += int(n)
print("samples by process command line:")
for cmd, n in procs.most_common(4):
    print(f"  {n:5}  {re.sub(r'/home/[^ ]*/pyspark/', '<pyspark>/', cmd)[:84]}")
print("top leaf frames in the forked Python workers:")
for leaf, n in leaves.most_common(5):
    print(f"  {n:5}  {leaf[:80]}")
PY
rm -rf "$out"
