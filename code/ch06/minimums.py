"""Chapter 6: the minimum versions that the installed pyspark 4.3.0 declares, and what this machine has.

Run from code/:  uv run python ch06/minimums.py
"""
import importlib.metadata as md
import platform
import re
import shutil
import subprocess

meta = md.metadata("pyspark")
print(f"pyspark {meta['Version']}   Requires-Python {meta['Requires-Python']}   "
      f"this Python {platform.python_version()}")

java = shutil.which("java")
jv = subprocess.run([java, "-version"], capture_output=True, text=True).stderr.splitlines()[0]
print(f"java on PATH: {jv}")

need = {}
for req in md.requires("pyspark"):
    m = re.match(r"([A-Za-z0-9_.-]+)([^;]*)(?:; extra == \"([^\"]+)\")?", req)
    name, spec, extra = m.group(1), m.group(2).strip(), m.group(3) or "(always)"
    need.setdefault((name, spec), []).append(extra)

print(f"\n{'package':<26}{'pyspark asks for':<22}{'installed here':<16}extras")
for (name, spec), extras in sorted(need.items()):
    try:
        have = md.version(name)
    except md.PackageNotFoundError:
        have = "-"
    print(f"{name:<26}{spec:<22}{have:<16}{', '.join(extras)}")
