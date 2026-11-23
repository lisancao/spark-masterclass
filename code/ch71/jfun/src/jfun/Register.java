package jfun;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/** Python has no API that takes an Aggregator. Call this from Python through the Py4J gateway. */
public final class Register {
  private Register() {}

  public static void all(SparkSession spark) {
    spark.udf().register("mean_fare", functions.udaf(new MeanFare(), Encoders.DOUBLE()));
    spark.udf().register("is_png_udf", new IsPngUdf(), DataTypes.BooleanType);
  }
}
