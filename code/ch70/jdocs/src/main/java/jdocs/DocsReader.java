package jdocs;

import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/** Runs in a task. next() advances, get() returns the current row. */
public class DocsReader implements PartitionReader<InternalRow> {
  private final List<FileEntry> files;
  private final String[] columns;      // the read schema after pruning, in order
  private final boolean needsHeader;   // open PNGs only if width or height was asked for
  private final boolean brokenName;
  private int i = -1;
  private long opened = 0;
  private InternalRow current;

  DocsReader(List<FileEntry> files, StructType schema, boolean brokenName) {
    this.files = files;
    this.columns = schema.fieldNames();
    this.brokenName = brokenName;
    boolean h = false;
    for (String c : columns) h |= c.equals("width") || c.equals("height");
    this.needsHeader = h;
  }

  @Override public boolean next() throws IOException {
    if (++i >= files.size()) return false;
    FileEntry f = files.get(i);
    int[] wh = null;
    if (needsHeader && f.kind.equals("image")) { wh = FileEntry.pngSize(f.path); opened++; }
    Object[] values = new Object[columns.length];
    for (int c = 0; c < columns.length; c++) {
      values[c] = switch (columns[c]) {
        case "path" -> UTF8String.fromString(f.path);   // StringType: UTF8String
        case "name" -> brokenName ? f.name : UTF8String.fromString(f.name);
        case "kind" -> UTF8String.fromString(f.kind);
        case "size" -> f.size;                          // LongType: a long
        case "modified" -> f.modifiedMicros;            // TimestampType: microseconds since the epoch
        case "width" -> wh == null ? null : wh[0];      // IntegerType: an int, or null
        case "height" -> wh == null ? null : wh[1];
        default -> throw new IllegalStateException("unknown column " + columns[c]);
      };
    }
    current = new GenericInternalRow(values);
    return true;
  }

  @Override public InternalRow get() { return current; }

  /** Spark reads this per task and adds it to the scan node's filesOpened metric. */
  @Override public CustomTaskMetric[] currentMetricsValues() {
    final long v = opened;
    return new CustomTaskMetric[] {new CustomTaskMetric() {
      public String name() { return "filesOpened"; }
      public long value() { return v; }
    }};
  }

  @Override public void close() {}
}
