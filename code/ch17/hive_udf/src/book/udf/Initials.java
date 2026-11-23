package book.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/** A simple (old-style) Hive UDF: the first letter of each word, "Upper West Side" -> "UWS". */
public class Initials extends UDF {
  public Text evaluate(Text s) {
    if (s == null) return null;
    StringBuilder b = new StringBuilder();
    for (String w : s.toString().split("[\\s/-]+")) {
      if (!w.isEmpty()) b.append(Character.toUpperCase(w.charAt(0)));
    }
    return new Text(b.toString());
  }
}
