package book.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/** A Hive GenericUDF: the two-letter code of a NYC borough name, or NULL. */
public class BoroughCode extends GenericUDF {
  private PrimitiveObjectInspector in;

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length != 1 || !(args[0] instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentException("borough_code takes one string argument");
    }
    in = (PrimitiveObjectInspector) args[0];
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    Object v = args[0].get();
    if (v == null) return null;
    switch (String.valueOf(in.getPrimitiveJavaObject(v)).trim().toLowerCase()) {
      case "manhattan": return "MN";
      case "brooklyn": return "BK";
      case "queens": return "QN";
      case "bronx": return "BX";
      case "staten island": return "SI";
      default: return null;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "borough_code(" + children[0] + ")";
  }
}
