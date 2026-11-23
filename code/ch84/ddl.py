"""Small helpers for the Chapter 84 scripts: run a statement and print it, catch an error, list files."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import re
import textwrap

UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def error_text(e, width=88, lines=2):
    """The error class and the first words of the message, wrapped (the first non-empty line)."""
    text = " ".join(l.strip() for l in str(e).splitlines() if l.strip())
    text = text.split(" JVM stacktrace:")[0]
    wrapped = textwrap.wrap(f"{type(e).__name__}: {text}", width - 2)
    return "\n".join("  " + w for w in wrapped[:lines]) + (" ..." if len(wrapped) > lines else "")


def runner(spark):
    def q(sql, n=30, show=True, width=88, err_lines=2):
        """Print the statement, run it, and show its rows (if any). An error prints its first lines."""
        lines = [l.strip() for l in sql.strip().splitlines()]
        print("> " + "\n     ".join(lines))
        try:
            df = spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            print(error_text(e, width, err_lines))
            return None
        if show and df.columns:
            df.show(n, truncate=False)
        return df
    return q


def files(root, label=None, pattern="*"):
    """Data files under root (no .crc, no hidden files), relative to root, with sizes."""
    root = Path(root)
    if label:
        print(f"  {label}:")
    if not root.exists():
        print("    (no folder)")
        return
    out = [p for p in sorted(root.rglob(pattern)) if p.is_file() and not p.name.startswith(".")]
    if not out:
        print("    (empty)")
    for p in out:
        name = str(p.relative_to(root))
        name = UUID.sub("<uuid>", name)
        print(f"    {name}  ({p.stat().st_size} B)")
