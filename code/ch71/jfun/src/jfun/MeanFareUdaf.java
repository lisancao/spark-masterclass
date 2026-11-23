package jfun;

import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.*;

/** The same mean as a UserDefinedAggregateFunction: deprecated since 3.0, still in 4.3. */
@SuppressWarnings("deprecation")
public class MeanFareUdaf extends UserDefinedAggregateFunction {
  @Override public StructType inputSchema() { return new StructType().add("x", DataTypes.DoubleType); }
  @Override public StructType bufferSchema() {
    return new StructType().add("sum", DataTypes.DoubleType).add("count", DataTypes.LongType);
  }
  @Override public DataType dataType() { return DataTypes.DoubleType; }
  @Override public boolean deterministic() { return true; }
  @Override public void initialize(MutableAggregationBuffer b) { b.update(0, 0.0); b.update(1, 0L); }
  @Override public void update(MutableAggregationBuffer b, Row in) {
    if (!in.isNullAt(0)) { b.update(0, b.getDouble(0) + in.getDouble(0)); b.update(1, b.getLong(1) + 1); }
  }
  @Override public void merge(MutableAggregationBuffer a, Row b) {
    a.update(0, a.getDouble(0) + b.getDouble(0)); a.update(1, a.getLong(1) + b.getLong(1));
  }
  @Override public Object evaluate(Row b) { return b.getLong(1) == 0 ? null : b.getDouble(0) / b.getLong(1); }
}
