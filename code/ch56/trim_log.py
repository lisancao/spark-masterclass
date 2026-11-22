"""Keep the plan change log readable: drop log4j prefixes, 'Output Information' blocks and start-up noise."""
import re, sys
skip = False
metrics = False
for line in sys.stdin:
    if line.startswith(("WARNING:", "Using Spark", "Setting default", "To adjust")) or "WARN Utils" in line \
            or "NativeCodeLoader" in line or "[Stage" in line or "SyntaxWarning" in line or line.startswith("  \"\"\""):
        continue
    if line.startswith("=== Metrics of Executed Rules") or line.startswith("Total number of") or line.startswith("Total time"):
        if not metrics:
            continue
    if line.startswith("=== Metrics of Analyzer"):
        metrics = True
    if line.startswith("Output Information:"):
        skip = True
        continue
    if skip:
        if line.strip() == "" or re.match(r"\d\d/\d\d/\d\d ", line):
            skip = False
        else:
            continue
    line = re.sub(r"^\d\d/\d\d/\d\d \d\d:\d\d:\d\d WARN PlanChangeLogger: ?", "", line)
    if line.strip() == "" :
        continue
    print(line.rstrip())
