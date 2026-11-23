"""A TRANSFORM script: read rows from stdin (one per line, fields split by a tab), write rows to stdout.
Used by ch17/transform.py:  SELECT TRANSFORM (id, zone) USING 'python3 zone_short.py' ..."""
import sys

for line in sys.stdin:
    fields = line.rstrip("\n").split("\t")
    zone = fields[1]
    short = "".join(w[0] for w in zone.replace("/", " ").split() if w)
    print(f"{fields[0]}\t{short.upper()}\t{len(zone)}")
