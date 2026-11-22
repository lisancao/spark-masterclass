"""Expand @@out:@@, @@json:@@ and @@code:file:a-b@@ markers into the chapter."""
import re, sys, textwrap
from pathlib import Path
CH = Path.home() / "spark-masterclass-parts/ch48-orchestration/code/ch48"
LANG = {".py": "python", ".sh": "bash", ".yaml": "yaml", ".sql": "sql"}
def rep(m):
    kind, arg = m.group(1), m.group(2)
    if kind in ("out", "json"):
        text = (CH / "output" / arg).read_text().rstrip("\n")
        return f"```{'json' if kind == 'json' else 'text'}\n{text}\n```"
    path, rng = arg.rsplit(":", 1)
    a, b = map(int, rng.split("-"))
    lines = (CH / path).read_text().splitlines()[a - 1:b]
    return f"```{LANG[Path(path).suffix]}\n{textwrap.dedent(chr(10).join(lines))}\n```"
src = Path(sys.argv[1]).read_text()
out = re.sub(r"^@@(out|json|code):([^@]+)@@$", rep, src, flags=re.M)
Path(sys.argv[2]).write_text(out)
