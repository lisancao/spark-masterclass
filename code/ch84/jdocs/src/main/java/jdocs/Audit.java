package jdocs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counters that a test reads through spark._jvm. This works only in a classic local session,
 * where the driver and the executors are one JVM.
 */
public final class Audit {
  static final AtomicInteger OPENED = new AtomicInteger();    // files opened by DocsReader
  static final AtomicInteger LISTINGS = new AtomicInteger();  // folder listings by build()
  static final AtomicInteger BUILDERS = new AtomicInteger();  // newScanBuilder calls
  static final AtomicInteger FULL_STATS = new AtomicInteger(); // estimateStatistics calls
  static final AtomicInteger SIZE_ONLY = new AtomicInteger();  // estimateSizeInBytes calls (4.3)
  static final List<String> OFFERED = new ArrayList<>();      // predicates offered to pushPredicates
  static final List<String> RUNTIME = new ArrayList<>();      // predicates given to filter() at run time

  public static synchronized void reset() {
    OPENED.set(0); LISTINGS.set(0); FULL_STATS.set(0); SIZE_ONLY.set(0); BUILDERS.set(0); OFFERED.clear(); RUNTIME.clear();
  }
  public static int opened() { return OPENED.get(); }
  public static int listings() { return LISTINGS.get(); }
  public static int fullStats() { return FULL_STATS.get(); }
  public static int sizeOnly() { return SIZE_ONLY.get(); }
  public static int builders() { return BUILDERS.get(); }
  public static synchronized String offered() { return String.join("\n", OFFERED); }
  public static synchronized String runtime() { return String.join("\n", RUNTIME); }
  static synchronized void offer(String s) { OFFERED.add(s); }
  static synchronized void runtime(String s) { RUNTIME.add(s); }
}
