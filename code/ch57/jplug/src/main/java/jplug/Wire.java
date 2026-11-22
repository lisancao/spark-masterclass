package jplug;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Plugins receive the serialized google.protobuf.Any as byte[]. Spark's own copy of
 * protobuf-java is shaded (org.sparkproject.connect.protobuf), so instead of depending
 * on it, this class reads the wire format directly: each field is a varint key
 * (field number << 3 | wire type) followed by a varint (type 0) or a length and that
 * many bytes (type 2: strings, bytes and embedded messages).
 */
public final class Wire {
  /** Field number -> value: byte[] for length-delimited fields, Long for varints. */
  public static Map<Integer, Object> fields(byte[] buf) {
    Map<Integer, Object> out = new HashMap<>();
    int[] pos = {0};
    while (pos[0] < buf.length) {
      long key = varint(buf, pos);
      int field = (int) (key >>> 3), type = (int) (key & 7);
      switch (type) {
        case 0 -> out.put(field, varint(buf, pos));
        case 2 -> {
          int len = (int) varint(buf, pos);
          byte[] v = new byte[len];
          System.arraycopy(buf, pos[0], v, 0, len);
          pos[0] += len;
          out.put(field, v);
        }
        case 1 -> pos[0] += 8;
        case 5 -> pos[0] += 4;
        default -> throw new IllegalArgumentException("unsupported wire type " + type);
      }
    }
    return out;
  }

  static long varint(byte[] b, int[] pos) {
    long r = 0;
    int shift = 0;
    while (true) {
      byte x = b[pos[0]++];
      r |= (long) (x & 0x7f) << shift;
      if ((x & 0x80) == 0) return r;
      shift += 7;
    }
  }

  public static String string(Map<Integer, Object> f, int n, String dflt) {
    Object v = f.get(n);
    return v == null ? dflt : new String((byte[]) v, StandardCharsets.UTF_8);
  }

  public static byte[] bytes(Map<Integer, Object> f, int n) {
    Object v = f.get(n);
    return v == null ? new byte[0] : (byte[]) v;
  }
}
