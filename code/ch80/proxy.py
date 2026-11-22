"""A gRPC proxy that logs every Spark Connect RPC that goes through it.

It forwards raw bytes, so it works for every method, and it decodes each message with the
protobuf classes that ship in pyspark (pyspark.sql.connect.proto) only to log a summary.

  uv run python ch80/proxy.py 15106 localhost:15105 rpc.jsonl     # listen, upstream, log file
"""
import json, sys, threading, time
from concurrent import futures
import grpc
from pyspark.sql.connect.proto import base_pb2 as pb

SERVICE = "/spark.connect.SparkConnectService/"
TYPES = {   # method -> (request class, response class), from base.proto
    "ExecutePlan": (pb.ExecutePlanRequest, pb.ExecutePlanResponse),
    "ReattachExecute": (pb.ReattachExecuteRequest, pb.ExecutePlanResponse),
    "AnalyzePlan": (pb.AnalyzePlanRequest, pb.AnalyzePlanResponse),
    "Config": (pb.ConfigRequest, pb.ConfigResponse),
    "AddArtifacts": (pb.AddArtifactsRequest, pb.AddArtifactsResponse),
    "ArtifactStatus": (pb.ArtifactStatusesRequest, pb.ArtifactStatusesResponse),
    "Interrupt": (pb.InterruptRequest, pb.InterruptResponse),
    "ReleaseExecute": (pb.ReleaseExecuteRequest, pb.ReleaseExecuteResponse),
    "ReleaseSession": (pb.ReleaseSessionRequest, pb.ReleaseSessionResponse),
    "FetchErrorDetails": (pb.FetchErrorDetailsRequest, pb.FetchErrorDetailsResponse),
    "CloneSession": (pb.CloneSessionRequest, pb.CloneSessionResponse),
    "GetStatus": (pb.GetStatusRequest, pb.GetStatusResponse),
}


def summary(msg):
    """The set fields of a message, and the chosen branch of each oneof, in a few words."""
    out = []
    for oneof in msg.DESCRIPTOR.oneofs:
        which = msg.WhichOneof(oneof.name)
        if which and not oneof.name.startswith("_"):     # "_x" oneofs only mark optional fields
            out.append(f"{oneof.name}={which}")
    if isinstance(msg, pb.ExecutePlanResponse):
        if msg.HasField("arrow_batch"):
            out.append(f"rows={msg.arrow_batch.row_count} arrow_bytes={len(msg.arrow_batch.data)}")
        if msg.HasField("schema"):
            out.append("schema")
        if msg.HasField("metrics"):
            out.append(f"metrics={len(msg.metrics.metrics)}")
    if isinstance(msg, pb.ExecutePlanRequest):
        out.append(f"plan={msg.plan.WhichOneof('op_type')}")
        if msg.tags:
            out.append(f"tags={list(msg.tags)}")
    if isinstance(msg, pb.AnalyzePlanRequest | pb.AnalyzePlanResponse):
        out.append(f"analyze={msg.WhichOneof('analyze')}" if isinstance(msg, pb.AnalyzePlanRequest)
                   else f"result={msg.WhichOneof('result')}")
    if isinstance(msg, pb.ConfigRequest):
        op = msg.operation.WhichOneof('op_type')
        keys = [p.key for p in getattr(getattr(msg.operation, op), "pairs", [])] or \
            list(getattr(getattr(msg.operation, op), "keys", []))
        out.append(f"op={op} keys={keys}")
    if isinstance(msg, pb.InterruptRequest):
        out.append(f"type={pb.InterruptRequest.InterruptType.Name(msg.interrupt_type)}")
    return " ".join(dict.fromkeys(o for o in out if o))


class Proxy(grpc.GenericRpcHandler):
    def __init__(self, upstream, log_path, save_dir=None):
        self.channel = grpc.insecure_channel(upstream, options=[
            ("grpc.max_receive_message_length", 256 << 20), ("grpc.max_send_message_length", 256 << 20)])
        self.log = open(log_path, "w", buffering=1)
        self.lock = threading.Lock()
        self.save_dir = save_dir
        self.t0 = time.time()
        self.n = 0

    def record(self, call, method, direction, raw, cls):
        try:
            msg = cls.FromString(raw)
            text = summary(msg)
        except Exception as e:   # noqa: BLE001
            msg, text = None, f"undecoded: {e}"
        with self.lock:
            self.log.write(json.dumps({"t": round(time.time() - self.t0, 4), "call": call,
                                       "method": method, "dir": direction,
                                       "bytes": len(raw), "summary": text}) + "\n")
            if self.save_dir and msg is not None:      # keep the first message of each kind
                if method == "ExecutePlan" and direction == "req":
                    p = self.save_dir / f"call{call}-ExecutePlanRequest.bin"
                    p.write_bytes(raw)
                if direction == "resp" and isinstance(msg, pb.ExecutePlanResponse) \
                        and msg.HasField("arrow_batch"):
                    p = self.save_dir / f"call{call}-arrow-batch.bin"
                    if not p.exists():
                        p.write_bytes(msg.arrow_batch.data)
                        (self.save_dir / f"call{call}-response.bin").write_bytes(raw)

    def service(self, details):
        method = details.method.removeprefix(SERVICE)
        req_cls, resp_cls = TYPES.get(method, (None, None))
        with self.lock:
            self.n += 1
            call = self.n
        upstream = self.channel.stream_stream(details.method)   # raw bytes in, raw bytes out
        md = [(k, v) for k, v in details.invocation_metadata]

        def handler(request_iterator, context):
            def forward():
                for raw in request_iterator:
                    self.record(call, method, "req", raw, req_cls)
                    yield raw
            responses = upstream(forward(), metadata=md)
            try:
                for raw in responses:
                    self.record(call, method, "resp", raw, resp_cls)
                    yield raw
            except grpc.RpcError as e:
                with self.lock:
                    self.log.write(json.dumps({"t": round(time.time() - self.t0, 4), "call": call,
                                               "method": method, "dir": "error", "bytes": 0,
                                               "summary": e.code().name}) + "\n")
                context.set_trailing_metadata(e.trailing_metadata() or ())
                context.abort(e.code(), e.details())
        return grpc.stream_stream_rpc_method_handler(handler)


def start(port, upstream, log_path, save_dir=None):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32), options=[
        ("grpc.max_receive_message_length", 256 << 20), ("grpc.max_send_message_length", 256 << 20)])
    proxy = Proxy(upstream, log_path, save_dir)
    server.add_generic_rpc_handlers((proxy,))
    server.add_insecure_port(f"localhost:{port}")
    server.start()
    return server, proxy


if __name__ == "__main__":
    port, upstream, log_path = sys.argv[1:4]
    server, _ = start(int(port), upstream, log_path)
    print(f"logging proxy on :{port} -> {upstream}, log in {log_path}")
    server.wait_for_termination()
