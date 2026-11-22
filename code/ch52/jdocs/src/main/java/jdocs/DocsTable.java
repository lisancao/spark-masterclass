package jdocs;

import java.util.EnumSet;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class DocsTable implements SupportsRead {
  private final StructType schema;
  DocsTable(StructType schema) { this.schema = schema; }

  @Override public String name() { return "jdocs"; }
  @Override public StructType schema() { return schema; }
  @Override public Set<TableCapability> capabilities() { return EnumSet.of(TableCapability.BATCH_READ); }

  /** Called for every scan during optimization: a fresh builder, so pushdown state cannot leak. */
  @Override public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    String path = options.get("path");
    if (path == null) throw new IllegalArgumentException("option 'path' is required: the folder to read");
    Audit.BUILDERS.incrementAndGet();
    return new DocsScanBuilder(path, schema, options);
  }
}
