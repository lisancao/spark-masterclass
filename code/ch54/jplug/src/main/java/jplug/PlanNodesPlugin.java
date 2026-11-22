package jplug;

import java.util.*;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.plans.QueryPlan;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.classic.Dataset;
import org.apache.spark.sql.classic.SparkSession;
import org.apache.spark.sql.connect.ConnectProtoUtils;
import org.apache.spark.sql.connect.planner.SparkConnectPlanner;
import org.apache.spark.sql.connect.plugin.RelationPlugin;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.jdk.javaapi.CollectionConverters;

/**
 * message jplug.PlanNodes { bytes input = 1; string stage = 2; }
 * input is a serialized spark.connect.Relation; stage is analyzed, optimized or physical.
 * Result: one row per plan node, depth first.
 */
public class PlanNodesPlugin implements RelationPlugin {
  public static final String TYPE_URL = "type.googleapis.com/jplug.PlanNodes";

  static final StructType SCHEMA = new StructType()
      .add("id", DataTypes.IntegerType, false)
      .add("parent", DataTypes.IntegerType, true)
      .add("depth", DataTypes.IntegerType, false)
      .add("node", DataTypes.StringType, false)
      .add("detail", DataTypes.StringType, false);

  @Override public Optional<LogicalPlan> transform(byte[] any, SparkConnectPlanner planner) {
    Map<Integer, Object> a = Wire.fields(any);                  // Any: 1 = type_url, 2 = value
    if (!TYPE_URL.equals(Wire.string(a, 1, ""))) return Optional.empty();   // not ours: next plugin
    Map<Integer, Object> msg = Wire.fields(Wire.bytes(a, 2));
    String stage = Wire.string(msg, 2, "optimized");
    Relation input = ConnectProtoUtils.parseRelationWithRecursionLimit(Wire.bytes(msg, 1), 1024);
    LogicalPlan child = planner.transformRelation(input);       // the client's DataFrame, server side
    SparkSession session = planner.session();
    QueryExecution qe = Dataset.ofRows(session, child).queryExecution();
    QueryPlan<?> root = switch (stage) {
      case "analyzed" -> qe.analyzed();
      case "optimized" -> qe.optimizedPlan();
      case "physical" -> qe.executedPlan();
      default -> throw new IllegalArgumentException(
          "stage must be analyzed, optimized or physical, got '" + stage + "'");
    };
    List<Row> rows = new ArrayList<>();
    walk(root, null, 0, rows);
    return Optional.of(session.createDataFrame(rows, SCHEMA).logicalPlan());
  }

  /** Depth-first walk. AdaptiveSparkPlanExec hides its plan from children(), so step into it. */
  static void walk(QueryPlan<?> p, Integer parent, int depth, List<Row> rows) {
    if (p instanceof AdaptiveSparkPlanExec a) p = a.executedPlan();
    int id = rows.size();
    rows.add(RowFactory.create(id, parent, depth, p.nodeName(), p.simpleString(400)));
    for (Object c : CollectionConverters.asJava(p.children())) walk((QueryPlan<?>) c, id, depth + 1, rows);
  }
}
