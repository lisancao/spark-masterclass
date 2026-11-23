"""Figures for Chapter 38. Numbers from code/ch38/output/committers.json and failure.txt."""
import json
from pathlib import Path

from svgkit import *

HERE = Path(__file__).resolve().parent
FIG = str(HERE.parents[1] / "book" / "media" / "figures") + "/"
data = {r["committer"]: r for r in json.loads((HERE.parent / "ch38" / "output" / "committers.json").read_text())}

# 1. Where one task's file goes, committer by committer (schematic paths, real request counts)
s = Svg(1000, 510, "The path of one output file for each committer")
rows = [
    ("file-v1", ["task attempt dir\n_temporary/.../attempt_*/", "job attempt dir\n_temporary/0/task_*/", "final key\npart-00000-*.parquet"],
     ["COPY + DELETE", "COPY + DELETE"], ["at task commit", "at job commit"], RED),
    ("file-v2", ["task attempt dir\n_temporary/.../attempt_*/", "final key\npart-00000-*.parquet"],
     ["COPY + DELETE"], ["at task commit"], ORANGE),
    ("magic", ["upload, not completed\n__magic_job-*/.../", "final key\npart-00000-*.parquet"],
     ["Complete upload"], ["at job commit"], GREEN),
    ("directory", ["local disk of the task\nfs.s3a.buffer.dir", "final key\npart-00000-*.parquet"],
     ["parts at task commit"], ["Complete at job commit"], TEAL),
]
y = 30
for name, boxes, a1, a2, col in rows:
    s.text(20, y + 48, name, 16, col, "start", 700, mono=True)
    x = 125
    for i, b in enumerate(boxes):
        last = i == len(boxes) - 1
        s.box(x, y + 12, 200, 64, b, col if last else GRAY, 12, mono=True)
        if not last:
            s.line(x + 202, y + 44, x + 268, y + 44, MUTE, 2)
            s.text(x + 235, y + 4, a1[i], 11, MUTE, "middle", 700)
            s.text(x + 235, y + 92, a2[i], 11, MUTE, "middle")
        x += 270
    r = data[name]["requests"]
    s.text(985, y + 40, f"COPY {r.get('CopyObject', 0)}", 13, col, "end", 700, mono=True)
    s.text(985, y + 58, f"MPU {r.get('NewMultipartUpload', 0)}", 13, col, "end", 700, mono=True)
    y += 115
s.text(20, 498, "Request counts for one write of 32 files (committers.txt). The folder names are shortened. "
       "partitioned works like directory.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch38-commit-paths.svg")

# 2. Requests by S3 API and the job commit time, for each committer
s = Svg(900, 400, "S3 requests and job commit time for one write of 32 files")
names = ["file-v1", "file-v2", "magic", "directory", "partitioned"]
groups = [("HEAD", ["HeadObject"], GRAY), ("LIST", ["ListObjectsV2", "ListMultipartUploads"], BLUE),
          ("PUT and parts", ["PutObject", "PutObjectPart", "NewMultipartUpload", "CompleteMultipartUpload"], GREEN),
          ("GET", ["GetObject"], PURPLE), ("COPY", ["CopyObject"], RED),
          ("DELETE", ["DeleteObject", "DeleteMultipleObjects"], ORANGE)]
X0, SC, BH = 120, 0.42, 34
for t in range(0, 1301, 250):
    s.line(X0 + t * SC, 40, X0 + t * SC, 300, MUTE, 0.6, dash=True, arrow=False)
    s.text(X0 + t * SC, 316, str(t), 11, MUTE)
s.text(X0 + 650 * SC, 334, "requests that MinIO received (mc admin trace)", 12, MUTE)
for i, n in enumerate(names):
    yy = 50 + i * 50
    s.text(X0 - 10, yy + 22, n, 14, MUTE, "end", 700, mono=True)
    x = X0
    req = data[n]["requests"]
    for lab, keys, col in groups:
        v = sum(req.get(k, 0) for k in keys)
        if v:
            s.rect(x, yy + 4, v * SC, BH - 8, col, 1)
            x += v * SC
    total = sum(req.values())
    s.text(x + 8, yy + 22, f"{total}", 13, MUTE, "start", 700)
    s.text(860, yy + 22, f"{data[n]['job commit s']:.2f} s", 13, ACC, "end", 700)
s.text(860, 34, "job commit", 12, ACC, "end", 700)
lx = X0
for lab, keys, col in groups:
    s.rect(lx, 352, 14, 14, col, 1)
    s.text(lx + 20, 364, lab, 12, MUTE, "start")
    lx += 115
s.text(20, 392, "Median job commit time of three writes after a warm-up, local[4], MinIO in Docker on the same machine.",
       11, MUTE, "start", italic=True)
s.save(FIG + "ch38-requests.svg")

# 3. After a failed job: what a reader finds (failure.txt)
s = Svg(960, 250, "What each committer leaves after task 7 fails")
cases = [("file-v1", 0, "nothing: the task output stayed under _temporary, and the job abort removed it", GRAY),
         ("file-v2", 7, "7 part files in the final folder, no _SUCCESS: a reader sees 70,000 rows", RED),
         ("magic", 0, "nothing: uploads were never completed, and the job abort cancelled them", GREEN)]
for i, (n, parts, txt, col) in enumerate(cases):
    yy = 30 + i * 70
    s.text(20, yy + 30, n, 16, col, "start", 700, mono=True)
    for k in range(8):
        filled = k < parts
        s.box(130 + k * 46, yy + 8, 40, 36, f"{k}", col if filled else GRAY, 12, opacity=1.0 if filled else 0.25,
              dash=not filled, stroke=None if filled else MUTE)
    s.text(510, yy + 30, txt, 12, MUTE, "start")
s.text(20, 238, "8 tasks of 10,000 rows. Task 7 fails after tasks 0 to 6 commit. Boxes: part files left in the destination.",
       11, MUTE, "start", italic=True)
s.save(FIG + "ch38-failure.svg")
print("ok")
