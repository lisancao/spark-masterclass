// Chapter 33: garbage collection with a cache of objects and with a serialized cache.
// Run through ch33/gc.sh (spark-shell -i), one JVM per configuration.
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel
import java.util.concurrent.atomic.AtomicLong
import book.Trip

val label = sys.env.getOrElse("GC_LABEL", "?")
val level = StorageLevel.fromString(sys.env.getOrElse("GC_LEVEL", "MEMORY_ONLY"))
val n = sys.env.getOrElse("GC_ROWS", "4000000").toInt
sc.setLogLevel("ERROR")
val gcMs = new AtomicLong(0); val runMs = new AtomicLong(0)
sc.addSparkListener(new SparkListener {
  override def onTaskEnd(e: SparkListenerTaskEnd): Unit = if (e.taskMetrics != null) {
    gcMs.addAndGet(e.taskMetrics.jvmGCTime); runMs.addAndGet(e.taskMetrics.executorRunTime)
  }
})
val vendors = Array("CMT", "VTS", "DDS", "MYC")
val trips = sc.parallelize(0 until n, 16).map(i => new Trip(i.toLong, i % 265, (i % 1000) / 10.0, vendors(i % 4))).persist(level)
trips.count()
val cached = sc.getRDDStorageInfo.find(_.id == trips.id).get
Thread.sleep(500); gcMs.set(0); runMs.set(0)
val t0 = System.nanoTime
for (_ <- 1 to 5) trips.map(t => (t.zone, t.distance)).reduceByKey(_ + _).count()
val secs = (System.nanoTime - t0) / 1e9
Thread.sleep(500)
println(f"$label%-34s cached ${cached.memSize / 1048576.0}%6.1f MiB (${cached.numCachedPartitions}/${cached.numPartitions}) | " +
        f"5 passes $secs%5.2f s | task GC time ${gcMs.get / 1000.0}%5.2f s of ${runMs.get / 1000.0}%6.2f s task time")
System.exit(0)
