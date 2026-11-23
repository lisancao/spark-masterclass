package jdocs;

import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.apache.spark.unsafe.types.UTF8String;

/** All files of one kind. The partition key is a promise: Spark does not check it. */
public class KindPartition extends DocsPartition implements HasPartitionKey {
  final String kind;
  KindPartition(String kind, List<FileEntry> files) { super(files); this.kind = kind; }
  @Override public InternalRow partitionKey() {
    return new GenericInternalRow(new Object[] {UTF8String.fromString(kind)});
  }
}
