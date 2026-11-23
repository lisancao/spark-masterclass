package jfun;

import org.apache.spark.sql.api.java.UDF1;

/** is_png as a Java UDF1. Spark wraps it in a ScalaUDF expression. */
public class IsPngUdf implements UDF1<byte[], Boolean> {
  static final byte[] SIG = {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'};

  public static boolean check(byte[] b) {
    if (b.length < SIG.length) return false;
    for (int i = 0; i < SIG.length; i++) if (b[i] != SIG[i]) return false;
    return true;
  }

  @Override public Boolean call(byte[] b) { return b == null ? null : check(b); }
}
