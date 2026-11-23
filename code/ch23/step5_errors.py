"""Step 5: a failed plan. The error class and SQLSTATE travel in the gRPC status details."""
import os

from miniconnect import Session, SparkError, col

TRIPS = os.path.join(os.environ.get("BOOK_DATA", os.path.expanduser("~/spark-masterclass/data")), "nyc-taxi", "yellow")
s = Session(os.environ.get("SPARK_REMOTE", "sc://localhost:15106"))
bad = s.read_parquet(TRIPS).select(col("revenue"))
print("built a plan over a column that does not exist; nothing was sent yet")
for what, call in [("AnalyzePlan", bad.schema), ("ExecutePlan", bad.to_arrow)]:
    try:
        call()
    except SparkError as e:
        print(f"{what}: error_class={e.error_class} sqlstate={e.sqlstate}")
        print(f"  {str(e)[:88]}")
s.close()

# What the trailers carry, without the wrapper: a google.rpc.Status with an ErrorInfo inside.
import grpc  # noqa: E402
from google.rpc import error_details_pb2, status_pb2  # noqa: E402
from miniconnect import base_pb2  # noqa: E402

s = Session(os.environ.get("SPARK_REMOTE", "sc://localhost:15106"))
bad = s.read_parquet(TRIPS).select(col("revenue"))
try:
    list(s.stub.ExecutePlan(s.request(bad.relation)))
except grpc.RpcError as e:
    print(f"\ngRPC status: {e.code().name}")
    trailers = dict(e.trailing_metadata())
    status = status_pb2.Status.FromString(trailers["grpc-status-details-bin"])
    info = error_details_pb2.ErrorInfo()
    status.details[0].Unpack(info)
    print(f"ErrorInfo.reason={info.reason} domain={info.domain}")
    for k in sorted(info.metadata):
        print(f"  metadata[{k}] = {info.metadata[k][:60]}")
    # The full error, with the server stack trace and causes, comes from FetchErrorDetails.
    details = s.stub.FetchErrorDetails(base_pb2.FetchErrorDetailsRequest(
        session_id=s.session_id, user_context=s.user, error_id=info.metadata["errorId"]))
    err = details.errors[details.root_error_idx]
    print(f"FetchErrorDetails: {len(details.errors)} error(s), root {err.error_type_hierarchy[0]},"
          f" {len(err.stack_trace)} stack frames")
s.close()
