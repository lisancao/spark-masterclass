"""Chapter 42: binaryFile, the 2 GiB limit, and the much lower limit that a 1g heap sets.

The out-of-memory case runs in a child process, because it kills its JVM.
Run from code/:  uv run python ch42/blobs.py
"""
import os, shutil, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DIR = Path("/tmp/masterclass-ch42/blobs")
MiB = 2 ** 20

if len(sys.argv) > 1:                        # child: read one file's content in a fresh 1g driver
    from pyspark.sql import functions as F
    from common.book import session
    spark = session("ch42-blob-child")
    n = spark.read.format("binaryFile").load(sys.argv[1]).select(F.length("content")).first()[0]
    print(f"read {n:,} bytes")
    sys.exit(0)

from pyspark.sql import functions as F
from common.book import session

shutil.rmtree(DIR, ignore_errors=True)
DIR.mkdir(parents=True)
huge = DIR / "huge.bin"
with open(huge, "wb") as f:
    f.truncate(2_500_000_000)                # sparse: no disk blocks are used
st = huge.stat()
print(f"huge.bin: apparent size {st.st_size:,} bytes, blocks on disk {st.st_blocks}")

spark = session("ch42-blobs")
df = spark.read.format("binaryFile").load(str(huge))
print("schema:", df.schema.simpleString())
print("select(path, length):", [(Path(r.path).name, r.length) for r in df.select("path", "length").collect()])
print("count():", df.count())
try:
    df.select(F.length("content")).collect()
except Exception as e:
    text = str(e)
    cause = next((l.strip() for l in text.splitlines() if "exceeds the max length" in l), text.splitlines()[0])
    cls = next((w for w in text.replace("[", " [").split() if w.startswith("[") and w.endswith("]")), "")
    print("select(content):", cls, cause.replace(str(DIR) + "/", "")[:140])

for size in (100, 300):
    with open(DIR / f"f{size}.bin", "wb") as f:
        f.write(os.urandom(size * MiB))

spark.conf.set("spark.sql.sources.binaryFile.maxLength", str(200 * MiB))
try:
    spark.read.format("binaryFile").load(str(DIR / "f300.bin")).select(F.length("content")).collect()
except Exception as e:
    line = next(l.strip() for l in str(e).splitlines() if "exceeds the max length" in l)
    print("with maxLength 200 MiB, f300.bin:", line.split("SparkException: ")[-1].replace("file:" + str(DIR) + "/", ""))
spark.stop()

print("\na fresh driver with the default 1g heap, one file each:")
for size in (100, 300):
    p = subprocess.run([sys.executable, __file__, str(DIR / f"f{size}.bin")], capture_output=True, text=True)
    oom = [l.strip() for l in p.stderr.splitlines() if "OutOfMemoryError" in l or "BufferHolder.grow" in l
           or "BinaryFileFormat" in l]
    print(f"  f{size}.bin ({size} MiB): exit code {p.returncode};",
          p.stdout.strip() or (oom[0] if oom else p.stderr.strip().splitlines()[-1][:100]))
    for l in oom[1:4]:
        print("      " + l[:110])
