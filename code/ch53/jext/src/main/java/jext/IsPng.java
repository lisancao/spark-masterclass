package jext;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext;
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback;
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.NullType;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * is_png(bytes): true if the bytes start with the 8-byte PNG signature, false if
 * not, NULL for a NULL input (the usual SQL rule for a function of one argument).
 *
 * A Scala case class gets equals, hashCode, Serializable and the Product methods
 * for free. A Java subclass of Expression writes them by hand.
 */
public class IsPng extends Expression implements CodegenFallback, Serializable {
  static final byte[] SIG = {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'};
  private final Expression child;

  public IsPng(Expression child) { this.child = child; }

  /** The check itself, also called directly by the StaticInvoke version. */
  public static boolean check(byte[] b) {
    if (b.length < SIG.length) return false;
    for (int i = 0; i < SIG.length; i++) if (b[i] != SIG[i]) return false;
    return true;
  }

  /** Interpreted evaluation: BinaryType values are byte[]; a NULL child gives NULL. */
  @Override public Object eval(InternalRow row) {
    Object v = child.eval(row);
    return v == null ? null : check((byte[]) v);
  }

  /** CodegenFallback: generated code calls eval(). Java must ask for the trait's method by name. */
  @Override public ExprCode doGenCode(CodegenContext ctx, ExprCode ev) {
    return CodegenFallback.super.doGenCode(ctx, ev);
  }

  /**
   * Runs during analysis: a wrong type fails with DATATYPE_MISMATCH, not a ClassCastException
   * in a task. NullType is the type of an untyped NULL literal, as in is_png(NULL).
   */
  @Override public TypeCheckResult checkInputDataTypes() {
    DataType t = child.dataType();
    if (t instanceof BinaryType || t instanceof NullType) return TypeCheckResult.TypeCheckSuccess$.MODULE$;
    return new TypeCheckResult.TypeCheckFailure("is_png expects BINARY, got " + child.dataType().simpleString());
  }

  @Override public DataType dataType() { return DataTypes.BooleanType; }
  @Override public boolean nullable() { return child.nullable(); }
  @Override public String prettyName() { return "is_png"; }

  // Tree plumbing that a Scala case class would generate.
  @Override public Seq<Expression> children() { return CollectionConverters.asScala(List.of(child)).toSeq(); }
  @Override public Expression withNewChildrenInternal(IndexedSeq<Expression> c) { return new IsPng(c.apply(0)); }
  @Override public int productArity() { return 1; }
  @Override public Object productElement(int n) {
    if (n == 0) return child;
    throw new IndexOutOfBoundsException(n);
  }
  @Override public boolean canEqual(Object o) { return o instanceof IsPng; }
  @Override public boolean equals(Object o) { return o instanceof IsPng p && p.child.equals(child); }
  @Override public int hashCode() { return Objects.hash("is_png", child); }
}
