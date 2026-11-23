"""Chapter 89, Steps 4-6: record the bytes between the JVM and a Python worker, and decode them.

Reuse is off, so each task gets its own worker and its own pair of tap files.
Run from code/:  uv run python ch89/tap.py
"""
import glob, os, pickle, shutil, sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.serializers import CloudPickleSerializer
from pyspark.sql import SparkSession, functions as F
from wire import parse_request, parse_response, hexdump

HERE = Path(__file__).resolve().parent
tapdir = tempfile.mkdtemp(prefix="ch89-tap-")
spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.python.worker.reuse", "false")            # one task per worker, so per file
         .config("spark.python.worker.module", "pyspark_tap")     # must start with "pyspark"
         .config("spark.executorEnv.PYTHONPATH", str(HERE / "tap"))
         .config("spark.executorEnv.TAP_DIR", tapdir)
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
src = spark.range(0, 3, 1, 1).selectExpr("concat('ab', id) AS s")

def capture(udf):
    for f in glob.glob(tapdir + "/*"):
        os.remove(f)
    rows = src.select(udf("s")).collect()
    ins = sorted(glob.glob(tapdir + "/*.in"), key=os.path.getsize)
    data_in = open(ins[-1], "rb").read()
    data_out = open(ins[-1][:-3] + ".out", "rb").read()
    return [r[0] for r in rows], data_in, data_out

out = Path(__file__).resolve().parent / "output"
raw = Path(__file__).resolve().parents[2] / "data" / "part10-tap"     # outside git: holds the secret
raw.mkdir(parents=True, exist_ok=True)
for label, udf in [("pickled", F.udf(lambda s: s.upper(), "string", useArrow=False)),
                   ("arrow", F.udf(lambda s: s.upper(), "string"))]:
    rows, data_in, data_out = capture(udf)
    (raw / f"{label}.in.bin").write_bytes(data_in)
    (raw / f"{label}.out.bin").write_bytes(data_out)
    marks = []
    req = parse_request(data_in, marks)
    secret = req["task_context"]["secret"].encode()
    shown_in = data_in.replace(secret, b"x" * len(secret))     # never print the task's secret
    print(f"===== {label} UDF: Spark returned {rows}")
    print(f"request: {len(data_in)} bytes into the worker, init message {req['init_length']} bytes")
    tc = req["task_context"]
    print(f"  partition {req['partition']}, Python {req['python_version']}, eval type {req['eval_type']}")
    print(f"  task context keys: {sorted(tc)}")
    print(f"  localProperties: {len(tc['localProperties'])} entries; secret: {len(tc['secret'] or '')} chars")
    print(f"  includes {req['includes']}, broadcasts {req['broadcasts']}")
    print(f"  runner conf: {req['runner_conf']}")
    print(f"  eval conf:   {req['eval_conf']}")
    u = req["udfs"][0]
    print(f"  UDFs: {len(req['udfs'])}; args {u['args']}; chained functions {len(u['commands'])}; "
          f"command {len(u['commands'][0])} bytes; result id {u['result_id']}")
    func, ret = CloudPickleSerializer().loads(u["commands"][0])
    print(f"  the command unpickles to ({func.__name__}, {ret!r}); func('xy') = {func('xy')!r}")
    if "frames" in req:
        print(f"  data: {len(req['frames'])} frame(s); rows {pickle.loads(req['frames'][0])}")
    else:
        print(f"  data: Arrow IPC stream, schema {req['table'].schema.to_string()!r}, rows {req['table'].column(0).to_pylist()}")
    print("\nfirst bytes of the request:")
    print(hexdump(shown_in, 0, 64, marks))
    print("...\nthe UDF section to the end:")
    udf_at = [o for o, n in marks if n.startswith("UDFs")][0]
    cmd_end = udf_at + 4 + 4 + 5 + 4 + 4 + 16       # count, args, offset+flag, functions, length, first bytes
    print(hexdump(shown_in, udf_at, cmd_end, marks))
    print("...")
    data_at = [o for o, n in marks if n == "data section"][0]
    print(hexdump(shown_in, data_at - 8, len(data_in), marks))
    rmarks = []
    resp = parse_response(data_out, rmarks)
    print(f"\nresponse: {len(data_out)} bytes out of the worker")
    print(hexdump(data_out, 0, len(data_out), rmarks))
    res = pickle.loads(resp["frames"][0]) if resp["frames"] else resp["table"].column(0).to_pylist()
    print(f"  results {res}; init {resp['init_ms'] - resp['boot_ms']} ms after boot; "
          f"processing {resp['processing_ms']} ms; spill {resp['spill']}; accumulators {len(resp['accumulators'])}\n")
shutil.rmtree(tapdir)
