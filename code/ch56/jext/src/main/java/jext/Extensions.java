package jext;

import java.util.List;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo;
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.types.AbstractDataType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import scala.Option;
import scala.Tuple3;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/** spark.sql.extensions=jext.Extensions: Spark calls apply once per new session. */
public class Extensions extends AbstractFunction1<SparkSessionExtensions, BoxedUnit> {
  @Override public BoxedUnit apply(SparkSessionExtensions ext) {
    // 1. is_png(bytes): our own Expression class
    ext.injectFunction(new Tuple3<>(
        new FunctionIdentifier("is_png"),
        new ExpressionInfo(IsPng.class.getName(), null, "is_png",
            "is_png(bytes) - true if bytes start with the PNG signature", ""),
        (Function1<Seq<Expression>, Expression>) args -> {
          if (args.size() != 1) throw new IllegalArgumentException("is_png takes 1 argument, got " + args.size());
          return new IsPng(args.apply(0));
        }));

    // 2. is_png_fast(bytes): a StaticInvoke of IsPng.check, which Spark compiles into generated code
    ext.injectFunction(new Tuple3<>(
        new FunctionIdentifier("is_png_fast"),
        new ExpressionInfo(IsPng.class.getName(), "is_png_fast"),
        (Function1<Seq<Expression>, Expression>) args -> new StaticInvoke(
            IsPng.class, DataTypes.BooleanType, "check", args,
            CollectionConverters.asScala(List.<AbstractDataType>of(DataTypes.BinaryType)).toSeq(),
            true, true, true, Option.empty())));

    // 3. The optimizer rule
    ext.injectOptimizerRule(session -> new PngRule());

    // 4. A check rule: runs on every analyzed plan and throws to reject it
    ext.injectCheckRule(session -> plan -> {
      plan.foreach(node -> {
        if (node instanceof Aggregate a) {
          a.groupingExpressions().foreach(g -> {
            if (g.resolved() && g.dataType() instanceof BinaryType)
              throw new IllegalArgumentException("Grouping by a BINARY column (" + g.sql()
                  + ") compares whole file contents; group by sha2(" + g.sql() + ", 256) instead");
            return BoxedUnit.UNIT;
          });
        }
        return BoxedUnit.UNIT;
      });
      return BoxedUnit.UNIT;
    });
    return BoxedUnit.UNIT;
  }
}
