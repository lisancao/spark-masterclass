package jdocs;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** spark.read.format("jdocs") finds this class through META-INF/services. Runs on the driver. */
public class DocsSource implements TableProvider, DataSourceRegister {
  public static final StructType SCHEMA = new StructType()
      .add("path", DataTypes.StringType, false)
      .add("name", DataTypes.StringType, false)
      .add("kind", DataTypes.StringType, false)
      .add("size", DataTypes.LongType, false)
      .add("modified", DataTypes.TimestampType, false)
      .add("width", DataTypes.IntegerType, true)
      .add("height", DataTypes.IntegerType, true);

  @Override public String shortName() { return "jdocs"; }

  /** Called by load(). The schema is fixed, so the options are not needed here. */
  @Override public StructType inferSchema(CaseInsensitiveStringMap options) { return SCHEMA; }

  @Override public Table getTable(StructType schema, Transform[] partitioning,
                                  Map<String, String> properties) {
    return new DocsTable(schema);
  }
}
