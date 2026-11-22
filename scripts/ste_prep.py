import re, sys
t = open(sys.argv[1]).read()
out = []; infence = False
for line in t.splitlines():
    if line.startswith("```"):
        infence = not infence; out.append(line); continue
    if infence: out.append(line); continue
    if re.match(r"\s*<(video|source|/video|figure|/figure)", line):
        out.append(""); continue
    line = re.sub(r"\]\([^)]*\)", "]", line)
    line = re.sub(r"\{[^}]*\.md-button[^}]*\}", "", line)
    line = line.replace("**", "")
    line = re.sub(r"</?[a-z]+[^>]*>", "", line)
    if re.match(r"\s*(?:\d+\.|[-*])\s", line):
        out.append("")
    out.append(line)
print("\n".join(out))
