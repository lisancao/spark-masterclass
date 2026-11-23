"""Chapter 55: what is in the Iceberg runtime jar, and a first statement through it.

Run from code/:  uv run python ch55/runtime.py
"""
import sys
import zipfile
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ice import ICEBERG_JAR, ice_session

jar = zipfile.ZipFile(ICEBERG_JAR)
names = jar.namelist()
classes = [n for n in names if n.endswith(".class")]
print(f"{ICEBERG_JAR.name}: {ICEBERG_JAR.stat().st_size:,} bytes, {len(classes):,} classes\n")

def group(n):
    if n.startswith("org/apache/iceberg/shaded/"):
        return "org.apache.iceberg.shaded.*  (Parquet, Avro, ORC, Arrow, ...)"
    if n.startswith("org/apache/iceberg/relocated/"):
        return "org.apache.iceberg.relocated.*  (Guava)"
    if n.startswith("org/apache/iceberg/"):
        return "org.apache.iceberg.*  (Iceberg itself)"
    if n.startswith("org/apache/spark/"):
        return "org.apache.spark.*  (extension rules)"
    if n.startswith("org/projectnessie/"):
        return "org.projectnessie.*  (Nessie client)"
    if n.startswith("org/locationtech/jts/"):
        return "org.locationtech.jts.*  (geometry, not relocated)"
    return "other"

for g, n in Counter(map(group, classes)).most_common():
    print(f"  {n:>6,}  {g}")
print(f"  {sum(n.startswith('software/amazon/') for n in classes):>6,}  software.amazon.*  (AWS SDK)")
print(f"  {sum(n.startswith('org/apache/iceberg/aws/') for n in classes):>6,}  org.apache.iceberg.aws.*  (S3FileIO, GlueCatalog)")

shaded = Counter(n.split("/")[5] + "." + n.split("/")[6] for n in classes
                 if n.startswith("org/apache/iceberg/shaded/org/apache/"))
print("\nshaded org.apache libraries:", ", ".join(f"{k} {v:,}" for k, v in shaded.most_common(5)))

svc = "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister"
entries = [l for l in jar.read(svc).decode().splitlines() if l.strip() and not l.startswith("#")]
print(f"\n{svc}:\n  " + "\n  ".join(entries))
props = dict(l.split("=", 1) for l in jar.read("iceberg-build.properties").decode().splitlines() if "=" in l)
print("\niceberg-build.properties:")
for k in ["git.branch", "git.closest.tag.name", "git.commit.id.abbrev", "git.commit.message.short",
          "git.build.version"]:
    print(f"  {k} = {props[k]}")

spark = ice_session("ch55-runtime", {"lake": "hadoop"})
print(f"\nSpark {spark.version}")
print("SELECT lake.system.iceberg_version():",
      spark.sql("SELECT lake.system.iceberg_version() AS v").first().v)

# The released 1.12.0 runtime for Spark 4.1, as wrong_jar.py downloads it, for comparison.
official = next(Path.home().glob(".ivy2*/cache/org.apache.iceberg/iceberg-spark-runtime-4.1_2.13/"
                                 "jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar"), None)
if official:
    on = [n for n in zipfile.ZipFile(official).namelist() if n.endswith(".class")]
    print(f"\nfor comparison, {official.name}: {official.stat().st_size:,} bytes, {len(on):,} classes")
    for label, cl in (("1.12.0 for 4.1", on), ("b1eab64 for 4.3", classes)):
        print(f"  {label:<16} org.eclipse.collections.*: {sum(n.startswith('org/eclipse/collections/') for n in cl):>5,}"
              f"   software.amazon.*: {sum(n.startswith('software/amazon/') for n in cl)}")
