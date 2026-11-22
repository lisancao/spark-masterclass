"""Chapter 36: print each pipeline command that the spark-pipelines CLI sends to the server.

  python ch36/trace_cli.py <spec> [run|dry-run] [CLI options]
Wraps SparkConnectClient.execute_command and execute_command_as_iterator, prints a one-line
summary of each PipelineCommand, then calls the real CLI. SPARK_REMOTE names the server.
"""
import sys

from pyspark.pipelines import cli
from pyspark.sql.connect.client.core import SparkConnectClient


def summary(cmd):
    kind = cmd.WhichOneof("command_type")
    c = getattr(cmd, kind)
    if kind == "create_dataflow_graph":
        return f"{kind}(default_catalog={c.default_catalog or None}, default_database={c.default_database or None})"
    if kind == "define_output":
        out_type = type(c).OutputType.Name(c.output_type) if hasattr(type(c), "OutputType") else c.output_type
        from pyspark.sql.connect.proto import OutputType
        return f"{kind}({c.output_name}, {OutputType.Name(c.output_type)})"
    if kind == "define_flow":
        detail = c.WhichOneof("details")
        rel = c.relation_flow_details.relation.WhichOneof("rel_type") if detail == "relation_flow_details" else ""
        return f"{kind}({c.flow_name} -> {c.target_dataset_name}, {detail}, plan root: {rel})"
    if kind == "define_sql_graph_elements":
        return f"{kind}({c.sql_file_path}, {len(c.sql_text)} characters of SQL)"
    if kind == "start_run":
        return f"{kind}(dry={c.dry}, full_refresh_all={c.full_refresh_all}, storage=.../{c.storage.rsplit('/', 1)[-1]})"
    return kind


def traced(real):
    def wrapper(self, command, *a, **k):
        if command.HasField("pipeline_command"):
            print("  -> " + summary(command.pipeline_command), flush=True)
        return real(self, command, *a, **k)
    return wrapper


SparkConnectClient.execute_command = traced(SparkConnectClient.execute_command)
SparkConnectClient.execute_command_as_iterator = traced(SparkConnectClient.execute_command_as_iterator)
spec, command = sys.argv[1], sys.argv[2]
sys.argv = ["spark-pipelines", command, "--spec", spec] + sys.argv[3:]
cli.main()
