package jdocs;

import java.util.*;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark pushes in this order: filters, aggregate, limit, then column pruning. Then build(). */
public class DocsScanBuilder implements SupportsPushDownV2Filters, SupportsPushDownRequiredColumns,
    SupportsPushDownLimit, SupportsPushDownAggregates {

  private final String root;
  private final CaseInsensitiveStringMap options;
  private StructType required;
  private final Filters filters = new Filters();
  private final List<Predicate> pushed = new ArrayList<>();
  private int limit = -1;
  private boolean countStar = false;
  private boolean groupByKind = false;

  DocsScanBuilder(String root, StructType schema, CaseInsensitiveStringMap options) {
    this.root = root; this.required = schema; this.options = options;
  }

  /** Keep what Filters accepts; return the rest, unchanged, for Spark to evaluate after the scan. */
  @Override public Predicate[] pushPredicates(Predicate[] predicates) {
    List<Predicate> rest = new ArrayList<>();
    for (Predicate p : predicates) {
      boolean took = filters.accept(p);
      Audit.offer((took ? "pushed   " : "returned ") + Filters.shape(p) + "  |  " + p);
      if (took) pushed.add(p); else rest.add(p);
    }
    return rest.toArray(new Predicate[0]);
  }

  @Override public Predicate[] pushedPredicates() { return pushed.toArray(new Predicate[0]); }

  /** Only the columns the query uses. readSchema() and the reader must honour it. */
  @Override public void pruneColumns(StructType requiredSchema) { this.required = requiredSchema; }

  /** One file is one row and every pushed filter is exact, so a limit on the listing is complete. */
  @Override public boolean pushLimit(int limit) { this.limit = limit; return true; }
  @Override public boolean isPartiallyPushed() { return false; }

  /** COUNT(*) alone, or COUNT(*) GROUP BY kind: both answerable from the listing. */
  private static boolean answerable(Aggregation agg) {
    AggregateFunc[] fs = agg.aggregateExpressions();
    Expression[] gs = agg.groupByExpressions();
    if (fs.length != 1 || !(fs[0] instanceof CountStar)) return false;
    return gs.length == 0 || (gs.length == 1 && "kind".equals(Filters.column(gs[0])));
  }

  /** true: Spark removes its own Aggregate and trusts our rows as the final answer. */
  @Override public boolean supportCompletePushDown(Aggregation agg) { return answerable(agg); }

  @Override public boolean pushAggregation(Aggregation agg) {
    if (!answerable(agg)) return false;
    countStar = true;
    groupByKind = agg.groupByExpressions().length == 1;
    return true;
  }

  @Override public Scan build() {
    Audit.LISTINGS.incrementAndGet();
    List<FileEntry> files = new ArrayList<>();
    for (FileEntry f : FileEntry.list(root)) if (filters.matches(f)) files.add(f);
    if (countStar) return new CountScan(files, groupByKind, pushed);
    if (limit >= 0 && files.size() > limit) files = new ArrayList<>(files.subList(0, limit));
    return new DocsScan(files, required, pushed, limit, options);
  }
}
