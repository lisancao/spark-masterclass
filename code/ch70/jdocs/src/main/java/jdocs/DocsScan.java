package jdocs;

import java.util.*;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomSumMetric;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The planned read: Scan and Batch in one class, which is common for small sources. Runs on the driver.
 * It also reports statistics and partitioning, and accepts runtime filters from a join.
 */
public class DocsScan implements Scan, Batch, SupportsReportStatistics, SupportsReportPartitioning,
    SupportsRuntimeV2Filtering {
  private List<FileEntry> files;
  private final StructType schema;
  private final List<Predicate> pushed;
  private final List<Predicate> runtime = new ArrayList<>();
  private final int limit;
  private final int maxPartitions;
  private final boolean byKind;       // option byKind=true: one partition per kind
  private final boolean brokenName;   // option brokenName=true: put a java.lang.String in a row
  private final long rowBytes;        // option rowBytes: the size of one row that the statistics report

  DocsScan(List<FileEntry> files, StructType schema, List<Predicate> pushed, int limit,
           CaseInsensitiveStringMap options) {
    this.files = files; this.schema = schema; this.pushed = pushed; this.limit = limit;
    this.maxPartitions = options.getInt("partitions", 2);
    this.byKind = options.getBoolean("byKind", false);
    this.brokenName = options.getBoolean("brokenName", false);
    this.rowBytes = options.getLong("rowBytes", 120L);   // a rough width for one row of metadata columns
  }

  @Override public StructType readSchema() { return schema; }

  /** What explain() prints after "BatchScan jdocs[...]". Spark prints nothing about pushdown for you. */
  @Override public String description() {
    return "jdocs[files=" + files.size() + ", pushed=" + pushed + (limit >= 0 ? ", limit=" + limit : "") + "]";
  }

  @Override public Batch toBatch() { return this; }

  @Override public InputPartition[] planInputPartitions() {
    if (byKind && hasKind()) {
      TreeMap<String, List<FileEntry>> groups = new TreeMap<>();
      for (FileEntry f : files) groups.computeIfAbsent(f.kind, k -> new ArrayList<>()).add(f);
      List<InputPartition> out = new ArrayList<>();
      groups.forEach((k, g) -> out.add(new KindPartition(k, g)));
      return out.toArray(new InputPartition[0]);
    }
    // Largest file first into the lightest group, as in the Python docs source.
    int n = Math.max(1, Math.min(maxPartitions, files.size()));
    List<List<FileEntry>> groups = new ArrayList<>();
    long[] totals = new long[n];
    for (int i = 0; i < n; i++) groups.add(new ArrayList<>());
    List<FileEntry> bySize = new ArrayList<>(files);
    bySize.sort(Comparator.comparingLong((FileEntry f) -> -f.size).thenComparing(f -> f.path));
    for (FileEntry f : bySize) {
      int best = 0;
      for (int i = 1; i < n; i++) if (totals[i] < totals[best]) best = i;
      groups.get(best).add(f);
      totals[best] += f.size;
    }
    List<InputPartition> out = new ArrayList<>();
    for (List<FileEntry> g : groups) if (!g.isEmpty()) out.add(new DocsPartition(g));
    return out.toArray(new InputPartition[0]);
  }

  @Override public PartitionReaderFactory createReaderFactory() {
    return new DocsReaderFactory(schema, brokenName);
  }

  /** Reported after pushdown, so the optimizer sees the filtered row count (join and broadcast choices). */
  @Override public Statistics estimateStatistics() {
    Audit.FULL_STATS.incrementAndGet();
    long rows = files.size();
    long bytes = rows * rowBytes;
    return new Statistics() {
      public OptionalLong sizeInBytes() { return OptionalLong.of(bytes); }
      public OptionalLong numRows() { return OptionalLong.of(rows); }
    };
  }

  /** New in 4.3: the cheap path Spark takes when it needs only a size (CBO and plan stats off). */
  @Override public OptionalLong estimateSizeInBytes() {
    Audit.SIZE_ONLY.incrementAndGet();
    return OptionalLong.of(files.size() * rowBytes);
  }

  private boolean hasKind() { return Arrays.asList(schema.fieldNames()).contains("kind"); }

  /** One partition per kind, when asked for and when kind is in the output. Spark does not check it. */
  @Override public Partitioning outputPartitioning() {
    if (byKind && hasKind()) {
      long kinds = files.stream().map(f -> f.kind).distinct().count();
      return new KeyGroupedPartitioning(new Expression[] {Expressions.identity("kind")}, (int) kinds);
    }
    return new UnknownPartitioning(0);
  }

  /** Dynamic partition pruning: Spark may give us predicates on kind while the query runs. */
  @Override public NamedReference[] filterAttributes() {
    return hasKind() ? new NamedReference[] {Expressions.column("kind")} : new NamedReference[0];
  }

  @Override public void filter(Predicate[] predicates) {
    for (Predicate p : predicates) {
      Audit.runtime(p.toString());
      Filters f = new Filters();
      if (f.accept(p)) {            // IN or = on kind: drop the files that cannot match
        List<FileEntry> keep = new ArrayList<>();
        for (FileEntry e : files) if (f.matches(e)) keep.add(e);
        files = keep;
        runtime.add(p);
      }
    }
  }

  @Override public Predicate[] pushedPredicates() { return runtime.toArray(new Predicate[0]); }

  /** A metric that the SQL tab of the Spark UI shows on the scan node. */
  public static class FilesOpened extends CustomSumMetric {
    @Override public String name() { return "filesOpened"; }
    @Override public String description() { return "files opened by jdocs"; }
  }

  @Override public CustomMetric[] supportedCustomMetrics() { return new CustomMetric[] {new FilesOpened()}; }
}
