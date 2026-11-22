// Chapter 60, Step 11: a Java UDF in a jar that ADD JAR puts on the classpath.
package book;

import org.apache.spark.sql.api.java.UDF1;

public class Shout implements UDF1<String, String> {
  @Override
  public String call(String s) {
    return s == null ? null : s.toUpperCase() + "!";
  }
}
