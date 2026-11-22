"""The manifest committer: it refuses S3A, and on a local file system it shows its manifests.

The manifest committer is the committer that Hadoop binds to abfs:// and gs:// by default.
This machine has no Azure or Google account, so the script runs the same committer class on
file:// and keeps a copy of each task manifest (mapreduce.manifest.committer.diagnostics...).
"""
import json
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from s3 import MANIFEST, s3_session  # noqa: E402
from common.book import ROOT, TRIPS  # noqa: E402

WORK = ROOT / "data" / "work" / "ch87"
OUT = WORK / "manifest-out"
DIAG = WORK / "manifest-diag"
REPORT = WORK / "manifest-report"

for p in (OUT, DIAG, REPORT):
    shutil.rmtree(p, ignore_errors=True)

spark = s3_session("ch87-manifest", {
    "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a": MANIFEST,
    "spark.hadoop.mapreduce.outputcommitter.factory.scheme.file": MANIFEST,
    "spark.hadoop.mapreduce.manifest.committer.diagnostics.manifest.directory": DIAG.as_uri(),
    "spark.hadoop.mapreduce.manifest.committer.summary.report.directory": REPORT.as_uri(),
})
df = spark.read.parquet(str(TRIPS)).repartition(8)

print("== the manifest committer on s3a://")
try:
    df.write.mode("overwrite").parquet("s3a://book/manifest/")
except Exception as e:
    line = [ln for ln in str(e).splitlines() if "committer" in ln][0]
    print("  " + line.strip()[:150])

print("\n== the manifest committer on file://")
df.write.mode("overwrite").parquet(OUT.as_uri())
succ = json.loads((OUT / "_SUCCESS").read_text())
print(f"  _SUCCESS: {(OUT / '_SUCCESS').stat().st_size} bytes, name={succ['name']}")
print(f"  committer: {succ['committer']}")
print(f"  files listed: {len(succ['filenames'])}, success={succ['success']}")
stats = succ.get("iostatistics", {})
keep = ["committer_task_manifest_file_size", "committer_files_committed",
        "committer_bytes_committed", "op_rename", "op_load_all_manifests",
        "op_create_directories", "job_stage_rename_files"]
for section in ("counters", "maximums"):
    for k, v in sorted(stats.get(section, {}).items()):
        if any(k.startswith(x) for x in keep) and v and v != -1 and ".failures" not in k:
            print(f"    {section[:-1]:7} {k} = {v:,}")
print("  job commit stages that ran (counters in _SUCCESS):")
for k, v in sorted(stats.get("counters", {}).items()):
    if k.startswith("job_stage_") and not k.endswith(".failures") and v:
        print(f"    {k} = {v}")

print("\n== one task manifest (a copy from the diagnostics directory)")
mans = sorted(DIAG.rglob("*-manifest.json"))
print(f"  manifests saved: {len(mans)}")
m = json.loads(mans[0].read_text())
print(f"  {mans[0].name}")
print(f"    type: {m['type']}")
print(f"    taskID: {m['taskID']}")
print(f"    files: {len(m['files'])}")
f0 = m["files"][0]
print(f"      source: {'manifest-out/' + f0['s'].split('manifest-out/')[1]}")
print(f"      dest:   {'manifest-out/' + f0['d'].split('manifest-out/')[1]}")
print(f"      size:   {f0['z']}")
print(f"    directories: {len(m.get('directories', []))}")
print("\n  summary reports:", ", ".join(p.name[:40] for p in sorted(REPORT.iterdir())))
