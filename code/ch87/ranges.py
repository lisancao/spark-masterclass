"""The Range header of each GET when Spark counts one day of the table (mc admin trace -v)."""
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s3 import ACCESS, ENDPOINT, MC, SECRET, s3_session  # noqa: E402

spark = s3_session("ch87-ranges")
df = spark.read.parquet("s3a://book/trips_by_day/")
subprocess.run(["docker", "rm", "-f", "book-ch87-trace-v"], capture_output=True)
subprocess.run(["docker", "run", "-d", "--name", "book-ch87-trace-v", "--network", "host", "--entrypoint", "sh", MC,
                "-c", "mc alias set m %s %s %s >/dev/null && mc admin trace -v m" % (ENDPOINT, ACCESS, SECRET)],
               capture_output=True)
time.sleep(3)
print("rows on 2026-06-15:", df.where("pickup_day = '2026-06-15'").count())
time.sleep(2)
out = subprocess.run(["docker", "logs", "book-ch87-trace-v"], capture_output=True, text=True).stdout
subprocess.run(["docker", "rm", "-f", "book-ch87-trace-v"], capture_output=True)
for line in out.splitlines():
    if "GET /book" in line or "Range:" in line or "Content-Range" in line:
        print(line.replace("localhost:19055 ", "")[:150])
print("fs.s3a.readahead.range =", spark._jsc.hadoopConfiguration().get("fs.s3a.readahead.range"))
