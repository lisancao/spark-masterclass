package jplug;

import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Substring;
import org.apache.spark.sql.connect.ConnectProtoUtils;
import org.apache.spark.sql.connect.planner.SparkConnectPlanner;
import org.apache.spark.sql.connect.plugin.ExpressionPlugin;
import org.apache.spark.sql.types.DataTypes;

/** message jplug.HasMagic { bytes child = 1; bytes magic = 2; }  ->  substring(child, 1, n) = magic */
public class MagicPlugin implements ExpressionPlugin {
  @Override public Optional<Expression> transform(byte[] any, SparkConnectPlanner planner) {
    Map<Integer, Object> a = Wire.fields(any);
    if (!"type.googleapis.com/jplug.HasMagic".equals(Wire.string(a, 1, ""))) return Optional.empty();
    Map<Integer, Object> m = Wire.fields(Wire.bytes(a, 2));
    Expression child = planner.transformExpression(
        ConnectProtoUtils.parseExpressionWithRecursionLimit(Wire.bytes(m, 1), 1024));
    byte[] magic = Wire.bytes(m, 2);
    Expression prefix = new Substring(child, Literal.create(1, DataTypes.IntegerType),
        Literal.create(magic.length, DataTypes.IntegerType));
    return Optional.of(new EqualTo(prefix, Literal.create(magic, DataTypes.BinaryType)));
  }
}
