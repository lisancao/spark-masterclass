package jdocs;

import java.util.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.LocalScan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/** COUNT(*) [GROUP BY kind] from the listing, on the driver: no task runs and no file is opened. */
public class CountScan implements LocalScan {
  private final List<FileEntry> files;
  private final boolean byKind;
  private final List<Predicate> pushed;

  CountScan(List<FileEntry> files, boolean byKind, List<Predicate> pushed) {
    this.files = files; this.byKind = byKind; this.pushed = pushed;
  }

  /** Grouping columns first, then the aggregates, in the order of the Aggregation. */
  @Override public StructType readSchema() {
    StructType s = new StructType();
    if (byKind) s = s.add("kind", DataTypes.StringType, false);
    return s.add("count", DataTypes.LongType, false);
  }

  @Override public InternalRow[] rows() {
    if (!byKind) return new InternalRow[] {new GenericInternalRow(new Object[] {(long) files.size()})};
    TreeMap<String, Long> counts = new TreeMap<>();
    for (FileEntry f : files) counts.merge(f.kind, 1L, Long::sum);
    List<InternalRow> out = new ArrayList<>();
    counts.forEach((k, v) -> out.add(new GenericInternalRow(new Object[] {UTF8String.fromString(k), v})));
    return out.toArray(new InternalRow[0]);
  }

  @Override public String description() {
    return "jdocs COUNT(*)" + (byKind ? " GROUP BY kind" : "") + " from listing, pushed=" + pushed;
  }
}
