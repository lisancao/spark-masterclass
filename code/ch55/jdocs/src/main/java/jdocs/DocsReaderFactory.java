package jdocs;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

/** Serialized to executors; createReader runs once per task, on the executor. */
public class DocsReaderFactory implements PartitionReaderFactory {
  private final StructType schema;
  private final boolean brokenName;
  DocsReaderFactory(StructType schema, boolean brokenName) { this.schema = schema; this.brokenName = brokenName; }
  @Override public PartitionReader<InternalRow> createReader(InputPartition p) {
    return new DocsReader(((DocsPartition) p).files, schema, brokenName);
  }
}
