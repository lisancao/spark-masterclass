"""Chapter 86: the client side of the jplug plugins, for a Spark Connect session.

plan_nodes(df, stage)  a relation plugin: the plan of any DataFrame, as rows
has_magic(col, magic)  an expression plugin: a typed Column built from a custom message
note(spark, text)      a command plugin: changes state on the server

These classes subclass internal PySpark Connect classes (LogicalPlan, Expression):
treat them as code that can need changes on a Spark upgrade.
"""


def make_messages():
    """jplug.PlanNodes, jplug.HasMagic and jplug.Note as Python message classes, without protoc."""
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    FD = descriptor_pb2.FieldDescriptorProto
    fdp = descriptor_pb2.FileDescriptorProto(name="jplug/messages.proto", package="jplug", syntax="proto3")
    for name, fields in [("PlanNodes", [("input", FD.TYPE_BYTES), ("stage", FD.TYPE_STRING)]),
                         ("HasMagic", [("child", FD.TYPE_BYTES), ("magic", FD.TYPE_BYTES)]),
                         ("Note", [("text", FD.TYPE_STRING)])]:
        m = fdp.message_type.add(name=name)
        for number, (fname, ftype) in enumerate(fields, start=1):
            m.field.add(name=fname, number=number, type=ftype, label=FD.LABEL_OPTIONAL)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    return {n: message_factory.GetMessageClass(pool.FindMessageTypeByName("jplug." + n))
            for n in ("PlanNodes", "HasMagic", "Note")}


MESSAGES = make_messages()


def client_classes():
    """Classes that need pyspark.sql.connect (imported only when a Connect session exists)."""
    from pyspark.sql.connect.column import Column
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.expressions import Expression
    from pyspark.sql.connect.plan import LogicalPlan

    class PlanNodesRelation(LogicalPlan):
        """A Relation whose rel_type is extension: Any(jplug.PlanNodes)."""

        def __init__(self, child, stage):
            super().__init__(child)
            self._stage = stage

        def plan(self, session):
            rel = self._create_proto_relation()          # sets common.plan_id
            msg = MESSAGES["PlanNodes"](input=self._child.plan(session).SerializeToString(),
                                        stage=self._stage)
            rel.extension.Pack(msg)                       # type_url type.googleapis.com/jplug.PlanNodes
            return rel

    class HasMagicExpression(Expression):
        def __init__(self, child, magic):
            super().__init__()
            self._child = child
            self._magic = magic

        def to_plan(self, session):
            import pyspark.sql.connect.proto as pb

            e = pb.Expression()
            e.extension.Pack(MESSAGES["HasMagic"](child=self._child.to_plan(session).SerializeToString(),
                                                  magic=self._magic))
            return e

        def __repr__(self):
            return f"has_magic({self._child!r}, {self._magic!r})"

    def plan_nodes(df, stage="optimized"):
        """The plan of df at a stage, as a DataFrame of (id, parent, depth, node, detail)."""
        return DataFrame(PlanNodesRelation(df._plan, stage), df.sparkSession)

    def has_magic(col, magic):
        """True if the binary column starts with the bytes magic."""
        return Column(HasMagicExpression(col._expr, magic))

    def note(spark, text):
        """Run the jplug.Note command on the server."""
        import pyspark.sql.connect.proto as pb

        cmd = pb.Command()
        cmd.extension.Pack(MESSAGES["Note"](text=text))
        spark.client.execute_command(cmd)

    return plan_nodes, has_magic, note
