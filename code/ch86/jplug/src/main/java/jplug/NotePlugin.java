package jplug;

import java.util.Map;
import org.apache.spark.sql.connect.planner.SparkConnectPlanner;
import org.apache.spark.sql.connect.plugin.CommandPlugin;

/** message jplug.Note { string text = 1; }  ->  stores text in the session conf jplug.note. */
public class NotePlugin implements CommandPlugin {
  @Override public boolean process(byte[] any, SparkConnectPlanner planner) {
    Map<Integer, Object> a = Wire.fields(any);
    if (!"type.googleapis.com/jplug.Note".equals(Wire.string(a, 1, ""))) return false;
    String text = Wire.string(Wire.fields(Wire.bytes(a, 2)), 1, "");
    planner.session().conf().set("jplug.note", text);
    return true;
  }
}
