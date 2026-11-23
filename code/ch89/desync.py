"""Chapter 89, Step 7: the SPARK-58241 desync, replayed against the 4.3 worker's own init-message parser.

Takes a real Arrow request recorded by tap.py, rewrites its init message the way 4.2.0's columnar
runner wrote it (the input schema as a string in front of the UDF section), and feeds it to
pyspark.worker_message.WorkerInitInfo, once as 4.3 reads it (a finished, length-bounded buffer)
and once as an unbounded socket would deliver it.
Run tap.py first.  Run from code/:  uv run python ch89/desync.py
"""
import struct, sys, threading, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.messages.zero_copy_byte_stream import ZeroCopyByteStream
from pyspark.worker_message import WorkerInitInfo
from wire import FrameReader, parse_init

data = (Path(__file__).resolve().parents[2] / "data" / "part10-tap" / "arrow.in.bin").read_bytes()
r = FrameReader(data)
r.int()
body = r.take(r.int())

marks = []
req = parse_init(FrameReader(body), marks)
udf_at = [o for o, n in marks if n.startswith("UDFs")][0]
schema = req["eval_conf"]["input_type"].encode()
legacy = body[:udf_at] + struct.pack("!i", len(schema)) + schema + body[udf_at:]   # 4.2.0 columnar layout
print(f"schema JSON: {len(schema)} bytes; the worker will read that length as the number of UDFs")
print(f"then the first 4 bytes of the JSON, {schema[:4]!r} = {struct.unpack('!i', schema[:4])[0]:,}, "
      f"as the first UDF's argument count\n")

def attempt(label, finished, timeout=5):
    stream = ZeroCopyByteStream(memoryview(legacy))
    if finished:
        stream.finish()                       # what SparkSocketMessageReceiver does in 4.3
    result = {}
    def run():
        try:
            WorkerInitInfo.from_stream(stream)
            result["out"] = "parsed (wrong)"
        except BaseException as e:
            result["out"] = f"{type(e).__name__}: {str(e)[:70]}"
    t = threading.Thread(target=run, daemon=True)
    start = time.perf_counter(); t.start(); t.join(timeout)
    took = time.perf_counter() - start
    print(f"  {label:<44} {result.get('out', f'still blocked after {timeout} s'):<60} "
          f"({took * 1000:.1f} ms)" if "out" in result else
          f"  {label:<44} still blocked after {timeout} s, waiting for bytes that never come")

attempt("4.3: a finished, length-bounded message", finished=True)
attempt("4.2-style: an open stream, no length", finished=False)

good = ZeroCopyByteStream(memoryview(body)); good.finish()
info = WorkerInitInfo.from_stream(good)
print(f"\nthe unchanged message parses: eval type {info.eval_type}, {len(info.udf_info)} UDF, "
      f"eval conf keys {list(info.eval_conf)}")
