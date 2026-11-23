package jfun;

import java.io.Serializable;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

/** A typed Aggregator: the mean of a double column, NULL values skipped. Buffer = (sum, count). */
public class MeanFare extends Aggregator<Double, MeanFare.Buf, Double> {
  public static class Buf implements Serializable {
    private double sum;
    private long count;
    public Buf() {}
    public double getSum() { return sum; }
    public void setSum(double s) { sum = s; }
    public long getCount() { return count; }
    public void setCount(long c) { count = c; }
  }

  @Override public Buf zero() { return new Buf(); }
  @Override public Buf reduce(Buf b, Double x) {
    if (x != null) { b.sum += x; b.count += 1; }
    return b;
  }
  @Override public Buf merge(Buf a, Buf b) { a.sum += b.sum; a.count += b.count; return a; }
  @Override public Double finish(Buf b) { return b.count == 0 ? null : b.sum / b.count; }
  @Override public Encoder<Buf> bufferEncoder() { return Encoders.bean(Buf.class); }
  @Override public Encoder<Double> outputEncoder() { return Encoders.DOUBLE(); }
}
