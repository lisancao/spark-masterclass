package jfun;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/** is_png as a Hive GenericUDF. Hive passes each value through an ObjectInspector. */
public class IsPngHive extends GenericUDF {
  private transient BinaryObjectInspector in;

  @Override public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length != 1) throw new UDFArgumentException("is_png_hive takes 1 argument");
    if (!(args[0] instanceof BinaryObjectInspector))
      throw new UDFArgumentException("is_png_hive expects BINARY, got " + args[0].getTypeName());
    in = (BinaryObjectInspector) args[0];
    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] args) throws HiveException {
    Object v = args[0].get();
    if (v == null) return null;
    return IsPngUdf.check(in.getPrimitiveJavaObject(v));
  }

  @Override public String getDisplayString(String[] children) { return "is_png_hive(" + children[0] + ")"; }
}
