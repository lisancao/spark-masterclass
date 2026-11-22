package jext;

import java.util.List;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.PythonUDF;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.AbstractPartialFunction;

/**
 * Replaces a Python UDF called is_png by the native expression. It runs in the
 * optimizer's operator batches, before "Extract Python UDFs" turns Python UDFs
 * into BatchEvalPython / ArrowEvalPython nodes, so a rewritten UDF never reaches
 * a Python worker.
 */
public class PngRule extends Rule<LogicalPlan> {
  static int rewrites = 0;
  public static int rewrites() { return rewrites; }

  /**
   * Only a UDF we know: named is_png, deterministic, one BINARY argument, BOOLEAN
   * result. Anything else might mean something different, so leave it alone.
   */
  static boolean matches(Expression e) {
    if (!(e instanceof PythonUDF u)) return false;
    return u.name().equals("is_png")
        && u.udfDeterministic()
        && u.dataType().equals(DataTypes.BooleanType)
        && u.children().size() == 1
        && u.children().apply(0).dataType().equals(DataTypes.BinaryType);
  }

  /**
   * The Python is_png returns False for None; the native is_png returns NULL.
   * coalesce(is_png(x), false) keeps the UDF's answer for every input.
   */
  static Expression replacement(PythonUDF u) {
    Expression child = u.children().apply(0);
    return new Coalesce(CollectionConverters.asScala(
        List.<Expression>of(new IsPng(child), Literal.FalseLiteral())).toSeq());
  }

  @Override public LogicalPlan apply(LogicalPlan plan) {
    return (LogicalPlan) plan.transformAllExpressions(new AbstractPartialFunction<Expression, Expression>() {
      @Override public boolean isDefinedAt(Expression e) { return matches(e); }

      @Override @SuppressWarnings("unchecked")
      public <A1 extends Expression, B1> B1 applyOrElse(A1 e, Function1<A1, B1> otherwise) {
        if (!matches(e)) return otherwise.apply(e);
        rewrites++;
        return (B1) replacement((PythonUDF) e);
      }
    });
  }
}
