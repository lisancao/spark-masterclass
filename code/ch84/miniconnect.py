"""A minimal Spark Connect client: gRPC, the generated protobuf classes and pyarrow. No pyspark.

Chapter 84 builds this file one step at a time. Run ./gen.sh first: it compiles the v4.3.0
.proto files into build/gen, which this module puts on the import path.
"""
import os
import sys
import uuid

import grpc
import pyarrow as pa

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "build", "gen"))
from spark.connect import base_pb2, base_pb2_grpc, expressions_pb2, relations_pb2  # noqa: E402

CLIENT_TYPE = "miniconnect/0.1"


class SparkError(Exception):
    """An error from the server, with the error class and SQLSTATE taken from the gRPC status."""

    def __init__(self, message, error_class=None, sqlstate=None):
        super().__init__(message)
        self.error_class, self.sqlstate = error_class, sqlstate


def _error(e: grpc.RpcError) -> SparkError:
    """Read google.rpc.Status from the trailers: it carries an ErrorInfo with the error class."""
    from google.rpc import error_details_pb2, status_pb2
    for key, value in e.trailing_metadata() or ():
        if key == "grpc-status-details-bin":
            status = status_pb2.Status.FromString(value)
            for d in status.details:
                info = error_details_pb2.ErrorInfo()
                if d.Unpack(info):
                    return SparkError(status.message, info.metadata.get("errorClass"),
                                      info.metadata.get("sqlState"))
    return SparkError(e.details())


class Session:
    """One server session: a gRPC channel, a session ID that this client chooses, a user."""

    def __init__(self, url="sc://localhost:15106", user="book"):
        host_port = url.removeprefix("sc://").split("/")[0]
        self.channel = grpc.insecure_channel(host_port)
        self.stub = base_pb2_grpc.SparkConnectServiceStub(self.channel)
        self.session_id = str(uuid.uuid4())
        self.user = base_pb2.UserContext(user_id=user)

    # ---- the core: one plan in, a stream of responses out ------------------------------
    def request(self, relation: relations_pb2.Relation) -> base_pb2.ExecutePlanRequest:
        return base_pb2.ExecutePlanRequest(
            session_id=self.session_id, user_context=self.user, client_type=CLIENT_TYPE,
            operation_id=str(uuid.uuid4()), plan=base_pb2.Plan(root=relation))

    def responses(self, relation):
        try:
            yield from self.stub.ExecutePlan(self.request(relation))
        except grpc.RpcError as e:
            raise _error(e) from None

    def execute(self, relation) -> pa.Table:
        """Run a relation and decode every ArrowBatch into one pyarrow Table."""
        batches = []
        for r in self.responses(relation):
            if r.WhichOneof("response_type") == "arrow_batch":
                with pa.ipc.open_stream(r.arrow_batch.data) as reader:
                    batches.extend(reader)
        return pa.Table.from_batches(batches) if batches else pa.table({})

    def execute_reattachable(self, relation, log=print) -> pa.Table:
        """ExecutePlan as a reattachable operation: if the stream ends before ResultComplete,
        call ReattachExecute from the last response ID; at the end, ReleaseExecute frees the buffer."""
        req = self.request(relation)
        req.request_options.append(base_pb2.ExecutePlanRequest.RequestOption(
            reattach_options=base_pb2.ReattachOptions(reattachable=True)))
        stream, last, batches, done, n = self.stub.ExecutePlan(req), None, [], False, 0
        while not done:
            for r in stream:
                last, n = r.response_id, n + 1
                kind = r.WhichOneof("response_type")
                if kind == "arrow_batch":
                    with pa.ipc.open_stream(r.arrow_batch.data) as reader:
                        batches.extend(reader)
                elif kind == "result_complete":
                    done = True
            if not done:                    # the server closed the stream early: continue from `last`
                log(f"  stream ended after {n} responses without ResultComplete: ReattachExecute")
                stream = self.stub.ReattachExecute(base_pb2.ReattachExecuteRequest(
                    session_id=self.session_id, user_context=self.user, client_type=CLIENT_TYPE,
                    operation_id=req.operation_id, last_response_id=last))
        log(f"  ResultComplete after {n} responses: ReleaseExecute")
        self.stub.ReleaseExecute(base_pb2.ReleaseExecuteRequest(
            session_id=self.session_id, user_context=self.user, client_type=CLIENT_TYPE,
            operation_id=req.operation_id, release_all=base_pb2.ReleaseExecuteRequest.ReleaseAll()))
        return pa.Table.from_batches(batches)

    def schema(self, relation):
        """AnalyzePlan(schema): the server analyzes the plan and returns its DataType. Nothing runs."""
        req = base_pb2.AnalyzePlanRequest(
            session_id=self.session_id, user_context=self.user, client_type=CLIENT_TYPE,
            schema=base_pb2.AnalyzePlanRequest.Schema(plan=base_pb2.Plan(root=relation)))
        try:
            return self.stub.AnalyzePlan(req).schema.schema
        except grpc.RpcError as e:
            raise _error(e) from None

    # ---- a small DataFrame API on top --------------------------------------------------
    def sql(self, query):
        return DataFrame(self, relations_pb2.Relation(sql=relations_pb2.SQL(query=query)))

    def read_parquet(self, path):
        read = relations_pb2.Read(data_source=relations_pb2.Read.DataSource(format="parquet", paths=[path]))
        return DataFrame(self, relations_pb2.Relation(read=read))

    def close(self):
        self.stub.ReleaseSession(base_pb2.ReleaseSessionRequest(
            session_id=self.session_id, user_context=self.user, client_type=CLIENT_TYPE))
        self.channel.close()


# ---- expressions: a Column wraps an unresolved Expression ------------------------------
E = expressions_pb2.Expression


class Column:
    def __init__(self, expr: E):
        self.expr = expr

    def _fn(self, name, other):
        return Column(call(name, self, other))

    def __ge__(self, other): return self._fn(">=", other)
    def __lt__(self, other): return self._fn("<", other)
    def __and__(self, other): return self._fn("and", other)

    def alias(self, name):
        return Column(E(alias=E.Alias(expr=self.expr, name=[name])))

    def desc(self):
        return E.SortOrder(child=self.expr, direction=E.SortOrder.SORT_DIRECTION_DESCENDING,
                           null_ordering=E.SortOrder.SORT_NULLS_LAST)


def col(name):
    return Column(E(unresolved_attribute=E.UnresolvedAttribute(unparsed_identifier=name)))


def lit(v):
    if isinstance(v, Column):
        return v
    if isinstance(v, bool):
        return Column(E(literal=E.Literal(boolean=v)))
    if isinstance(v, int):
        return Column(E(literal=E.Literal(integer=v)))
    if isinstance(v, float):
        return Column(E(literal=E.Literal(double=v)))
    return Column(E(literal=E.Literal(string=str(v))))


def call(name, *args):
    """Any Spark function, by name. The server resolves the name, as it does for PySpark."""
    return E(unresolved_function=E.UnresolvedFunction(
        function_name=name, arguments=[lit(a).expr for a in args]))


def fn(name, *args):
    return Column(call(name, *args))


# ---- a DataFrame wraps a Relation; each method wraps it in one more node ----------------
R = relations_pb2


class DataFrame:
    def __init__(self, session, relation):
        self.session, self.relation = session, relation

    def _wrap(self, **node):
        return DataFrame(self.session, R.Relation(**node))

    def where(self, cond: Column):
        return self._wrap(filter=R.Filter(input=self.relation, condition=cond.expr))

    def select(self, *cols):
        return self._wrap(project=R.Project(input=self.relation, expressions=[c.expr for c in cols]))

    def group_by(self, *names):
        return GroupedData(self, [col(n).expr for n in names])

    def order_by(self, *orders):
        return self._wrap(sort=R.Sort(input=self.relation, order=list(orders), is_global=True))

    def limit(self, n):
        return self._wrap(limit=R.Limit(input=self.relation, limit=n))

    def schema(self):
        return self.session.schema(self.relation)

    def to_arrow(self) -> pa.Table:
        return self.session.execute(self.relation)


class GroupedData:
    def __init__(self, df, keys):
        self.df, self.keys = df, keys

    def agg(self, *cols):
        return self.df._wrap(aggregate=R.Aggregate(
            input=self.df.relation, group_type=R.Aggregate.GROUP_TYPE_GROUPBY,
            grouping_expressions=self.keys, aggregate_expressions=[c.expr for c in cols]))
