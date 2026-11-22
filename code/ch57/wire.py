"""Shared decoding for Chapter 57: a strict frame reader and parsers for the 4.3 worker protocol.

Field order from BasePythonRunner.Writer.open (core/.../api/python/PythonRunner.scala) and
WorkerInitInfo.from_stream (python/pyspark/worker_message.py) at v4.3.0.
"""
import io, json, struct

import pyarrow as pa

END_OF_DATA_SECTION, PYTHON_EXCEPTION_THROWN, TIMING_DATA, END_OF_STREAM = -1, -2, -3, -4
NULL, START_ARROW_STREAM, END_OF_MICRO_BATCH, START_OF_INIT_MESSAGE = -5, -6, -7, -8
SQL_BATCHED_UDF, SQL_ARROW_BATCHED_UDF = 100, 101


class ProtocolError(Exception):
    pass


class FrameReader:
    """Big-endian ints and longs, bools, and length-prefixed bytes, as pyspark/serializers.py reads them."""
    def __init__(self, data):
        self.data, self.pos = data, 0
    def remaining(self):
        return len(self.data) - self.pos
    def take(self, n):
        if n < 0 or n > self.remaining():
            raise ProtocolError(f"asked for {n} bytes at offset {self.pos}, {self.remaining()} left")
        out = self.data[self.pos:self.pos + n]
        self.pos += n
        return out
    def int(self):
        return struct.unpack("!i", self.take(4))[0]
    def long(self):
        return struct.unpack("!q", self.take(8))[0]
    def bool(self):
        return struct.unpack("!?", self.take(1))[0]
    def bytes(self):
        return self.take(self.int())
    def utf(self):
        return self.bytes().decode("utf-8")


def read_conf(r):
    return {r.utf(): r.utf() for _ in range(r.int())}


def parse_init(r, marks):
    """The init message body. `marks` collects (offset, field) for the annotated hexdump."""
    req = {}
    def mark(name):
        marks.append((r.pos, name))
    mark("partition index (int)"); req["partition"] = r.int()
    mark("Python version (string)"); req["python_version"] = r.utf()
    mark("task context (string, JSON)"); req["task_context"] = json.loads(r.utf())
    mark("Spark files root (string)"); req["files_root"] = r.utf()
    mark("includes (int count, strings)"); req["includes"] = [r.utf() for _ in range(r.int())]
    mark("broadcasts (bool, int count, ...)")
    if r.bool():
        raise ProtocolError("encrypted broadcasts are not handled by this reader")
    req["broadcasts"] = []
    for _ in range(r.int()):
        bid = r.long()
        req["broadcasts"].append((bid, r.utf() if bid >= 0 else None))
    mark("eval type (int)"); req["eval_type"] = r.int()
    mark("runner conf (int count, pairs)"); req["runner_conf"] = read_conf(r)
    mark("eval conf (int count, pairs)"); req["eval_conf"] = read_conf(r)
    mark("UDFs (int count, ...)")
    udfs = []
    for _ in range(r.int()):
        args = []
        for _ in range(r.int()):
            offset = r.int()
            args.append((offset, r.utf() if r.bool() else None))
        commands = [r.bytes() for _ in range(r.int())]
        udfs.append({"args": args, "commands": commands, "result_id": r.long()})
    req["udfs"] = udfs
    return req


def parse_request(data, marks=None):
    """START_OF_INIT_MESSAGE, its length, the init message, the data section, END_OF_STREAM."""
    marks = [] if marks is None else marks
    r = FrameReader(data)
    marks.append((0, "START_OF_INIT_MESSAGE (int -8)"))
    if r.int() != START_OF_INIT_MESSAGE:
        raise ProtocolError("request does not start with START_OF_INIT_MESSAGE")
    marks.append((4, "init message length (int)"))
    n = r.int()
    body = FrameReader(r.take(n))
    sub = []
    req = parse_init(body, sub)
    marks.extend((8 + off, name) for off, name in sub)
    if body.remaining():
        raise ProtocolError(f"{body.remaining()} bytes left in the init message")
    req["init_length"] = n
    marks.append((r.pos, "data section"))
    if req["eval_type"] == SQL_BATCHED_UDF:
        frames = []
        while (k := r.int()) != END_OF_DATA_SECTION:
            frames.append(r.take(k))
        req["frames"] = frames
    elif req["eval_type"] == SQL_ARROW_BATCHED_UDF:
        stream = io.BytesIO(data[r.pos:])
        req["table"] = pa.ipc.open_stream(stream).read_all()
        r.pos += stream.tell()
    else:
        raise ProtocolError(f"eval type {req['eval_type']} is not handled")
    marks.append((r.pos, "END_OF_STREAM (int -4)"))
    if r.int() != END_OF_STREAM:
        raise ProtocolError("request does not end with END_OF_STREAM")
    if r.remaining():
        raise ProtocolError(f"{r.remaining()} bytes after END_OF_STREAM")
    return req


def parse_response(data, marks=None):
    marks = [] if marks is None else marks
    r = FrameReader(data)
    resp = {"frames": [], "table": None}
    while True:
        at = r.pos
        n = r.int()
        if n == TIMING_DATA:
            marks.append((at, "TIMING_DATA (int -3), then 4 longs"))
            break
        if n == PYTHON_EXCEPTION_THROWN:
            raise ProtocolError("worker reported an exception: " + r.utf())
        if n == START_ARROW_STREAM:
            marks.append((at, "START_ARROW_STREAM (int -6), then an Arrow IPC stream"))
            stream = io.BytesIO(data[r.pos:])
            resp["table"] = pa.ipc.open_stream(stream).read_all()
            r.pos += stream.tell()
        else:
            marks.append((at, f"result frame ({n} bytes, a pickle)"))
            resp["frames"].append(r.take(n))
    resp["boot_ms"], resp["init_ms"], resp["finish_ms"], resp["processing_ms"] = (r.long() for _ in range(4))
    marks.append((r.pos, "memory and disk bytes spilled (2 longs)"))
    resp["spill"] = (r.long(), r.long())
    marks.append((r.pos, "END_OF_DATA_SECTION (int -1), then accumulators"))
    if r.int() != END_OF_DATA_SECTION:
        raise ProtocolError("no END_OF_DATA_SECTION after the timing data")
    resp["accumulators"] = [r.bytes() for _ in range(r.int())]
    marks.append((r.pos, "END_OF_STREAM (int -4)"))
    if r.int() != END_OF_STREAM:
        raise ProtocolError("response does not end with END_OF_STREAM")
    return resp


def hexdump(data, start=0, end=None, marks=()):
    """16 bytes per line, offsets in hex, with a field name on the line where each field starts."""
    end = len(data) if end is None else end
    names = {}
    for off, name in marks:
        names.setdefault(off // 16 * 16, []).append(name)
    out = []
    for off in range(start // 16 * 16, end, 16):
        chunk = data[off:off + 16]
        hx = " ".join(f"{b:02x}" for b in chunk)
        asc = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        note = "  <- " + "; ".join(names[off]) if off in names else ""
        out.append(f"{off:06x}  {hx:<47}  {asc:<16}{note}")
    return "\n".join(out)
