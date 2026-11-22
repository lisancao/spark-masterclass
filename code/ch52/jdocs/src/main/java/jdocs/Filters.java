package jdocs;

import java.io.Serializable;
import java.util.*;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.unsafe.types.UTF8String;

/** The predicates jdocs takes over. Every one is decided from listing metadata, before any file opens. */
public class Filters implements Serializable {
  Set<String> kinds = null;                            // null means any kind
  final List<String> nameSuffixes = new ArrayList<>();
  final List<Object[]> sizeTests = new ArrayList<>();  // {operator, Long value}

  static final Set<String> NEVER_NULL = Set.of("path", "name", "kind", "size", "modified");

  /** The column a predicate child names, or null if it is not a plain top-level column. */
  static String column(Expression e) {
    if (e instanceof NamedReference r && r.fieldNames().length == 1) return r.fieldNames()[0];
    return null;
  }

  /** A literal child as a plain Java value. Strings arrive as UTF8String, not String. */
  static Object value(Expression e) {
    if (!(e instanceof Literal<?> l)) return null;
    Object v = l.value();
    return v instanceof UTF8String u ? u.toString() : v;
  }

  /** name(child, child) with each child shown as a column, a typed literal or a nested name. */
  static String shape(Expression e) {
    if (e instanceof NamedReference r) return String.join(".", r.fieldNames());
    if (e instanceof Literal<?> l) return "lit:" + (l.value() == null ? "null" : l.value().getClass().getSimpleName());
    if (e instanceof GeneralScalarExpression g) {
      StringJoiner j = new StringJoiner(", ", g.name() + "(", ")");
      for (Expression c : g.children()) j.add(shape(c));
      return j.toString();
    }
    return e.getClass().getSimpleName();
  }

  /**
   * Returns true if jdocs takes over p. Spark then stops evaluating p, so
   * matches() must apply it exactly. Return false for anything else.
   */
  boolean accept(Predicate p) {
    Expression[] c = p.children();
    String col = c.length > 0 ? column(c[0]) : null;
    if (col == null) return false;
    switch (p.name()) {
      case "IS_NOT_NULL":
        return NEVER_NULL.contains(col);              // always true for these columns
      case "=":
        if (col.equals("kind") && value(c[1]) instanceof String s) { keepKinds(Set.of(s)); return true; }
        return sizeTest(col, "=", c[1]);
      case "IN": {
        if (!col.equals("kind")) return false;
        Set<String> s = new HashSet<>();
        for (int i = 1; i < c.length; i++) {
          if (!(value(c[i]) instanceof String v)) return false;
          s.add(v);
        }
        keepKinds(s);
        return true;
      }
      case "ENDS_WITH":
        if (col.equals("name") && value(c[1]) instanceof String s) { nameSuffixes.add(s); return true; }
        return false;
      case ">": case ">=": case "<": case "<=":
        return sizeTest(col, p.name(), c[1]);
      default:
        return false;
    }
  }

  void keepKinds(Set<String> s) {
    if (kinds == null) kinds = new HashSet<>(s); else kinds.retainAll(s);
  }

  private boolean sizeTest(String col, String op, Expression lit) {
    if (!col.equals("size") || !(value(lit) instanceof Long v)) return false;
    sizeTests.add(new Object[] {op, v});
    return true;
  }

  boolean matches(FileEntry f) {
    if (kinds != null && !kinds.contains(f.kind)) return false;
    for (String s : nameSuffixes) if (!f.name.endsWith(s)) return false;
    for (Object[] t : sizeTests) {
      long v = (Long) t[1];
      boolean ok = switch ((String) t[0]) {
        case "=" -> f.size == v;
        case ">" -> f.size > v;
        case ">=" -> f.size >= v;
        case "<" -> f.size < v;
        default -> f.size <= v;
      };
      if (!ok) return false;
    }
    return true;
  }
}
