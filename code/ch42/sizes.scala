// Chapter 42: how large JVM objects are, measured with Spark's SizeEstimator (the tool the tuning guide names).
// Run from code/:  .venv/lib/python3.12/site-packages/pyspark/bin/spark-shell --jars ch42/trip.jar -i ch42/sizes.scala | grep -E '^  [a-zA-Z]|^Size'
import org.apache.spark.util.SizeEstimator
import book.Trip
sc.setLogLevel("ERROR")
def show(label: String, raw: Long, o: AnyRef): Unit = {
  val est = SizeEstimator.estimate(o)
  println(f"  $label%-50s raw data $raw%,12d B   on the heap $est%,12d B   ${est.toDouble / raw}%5.1fx")
}
val n = 1000000
println(s"SizeEstimator.estimate on Java ${System.getProperty("java.version")}, heap 1g (compressed object pointers on)")
show("one Trip (long, int, double, 3-char String)", 8 + 4 + 8 + 3, new Trip(1L, 132, 2.5, "CMT"))
show("a 10-character String", 10, "Manhattan!")
show("Array[Int] of 1,000,000", 4L * n, Array.tabulate(n)(identity))
show("java.util.ArrayList[Integer] of 1,000,000", 4L * n, { val l = new java.util.ArrayList[Integer](n); (0 until n).foreach(i => l.add(i)); l })
show("Scala List[Int] of 1,000,000", 4L * n, List.tabulate(n)(identity))
show("java.util.HashMap[Integer,Integer] of 1,000,000", 8L * n, { val m = new java.util.HashMap[Integer, Integer](); (0 until n).foreach(i => m.put(i, i)); m })
show("Array[Trip] of 1,000,000 (one shared String)", 23L * n, Array.tabulate(n)(i => new Trip(i.toLong, i % 265, i / 10.0, "CMT")))
System.exit(0)
