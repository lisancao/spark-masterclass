package jdocs;

import java.util.List;
import org.apache.spark.sql.connector.read.InputPartition;

/** Serialized into each task: listing metadata, never file contents. */
public class DocsPartition implements InputPartition {
  final List<FileEntry> files;
  DocsPartition(List<FileEntry> files) { this.files = files; }
}
