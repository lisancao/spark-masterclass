// Chapter 33: what spark.serializer changes. Run through ch33/serialization.sh (spark-shell -i).
// One run = one serializer, because the serializer is fixed when the SparkContext starts.
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel
import java.util.concurrent.atomic.AtomicLong

case class Trip(id: Long, zone: Int, distance: Double, vendor: String)

val label = sys.env.getOrElse("SER_LABEL", "?")
val n = sys.env.getOrElse("SER_ROWS", "2000000").toInt
spark.sparkContext.setLogLevel("ERROR")

// Count the shuffle bytes and the GC time of the tasks with a listener.
val shuffleBytes = new AtomicLong(0); val gcMs = new AtomicLong(0); val runMs = new AtomicLong(0)
sc.addSparkListener(new SparkListener {
  override def onTaskEnd(e: SparkListenerTaskEnd): Unit = if (e.taskMetrics != null) {
    shuffleBytes.addAndGet(e.taskMetrics.shuffleWriteMetrics.bytesWritten)
    gcMs.addAndGet(e.taskMetrics.jvmGCTime); runMs.addAndGet(e.taskMetrics.executorRunTime)
  }
})
def reset(): Unit = { Thread.sleep(500); shuffleBytes.set(0); gcMs.set(0); runMs.set(0) }
def time[T](f: => T): (T, Double) = { val t0 = System.nanoTime; val r = f; (r, (System.nanoTime - t0) / 1e9) }

val vendors = Array("CMT", "VTS", "DDS", "MYC")
val trips = sc.parallelize(0 until n, 16).map(i => Trip(i.toLong, i % 265, (i % 1000) / 10.0, vendors(i % 4)))

// 1. A cache in serialized form: one byte array per partition, written by spark.serializer.
val ser = trips.persist(StorageLevel.MEMORY_ONLY_SER)
val (cnt, tCache) = try time(ser.count()) catch { case e: Exception =>
  val root = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toList.last
  println(f"$label%-22s FAILED: ${root.getClass.getSimpleName}: ${root.getMessage.take(150)}"); System.exit(0); (0L, 0.0) }
val serBytes = sc.getRDDStorageInfo.find(_.id == ser.id).map(_.memSize).getOrElse(0L)

// 2. An RDD shuffle of the objects: the map output is written by spark.serializer.
reset()
val (_, tShuffle) = time(trips.map(t => (t.zone, t)).groupByKey(16).mapValues(_.size).count())
val rddShuffle = shuffleBytes.get

// 3. The same rows as a DataFrame shuffle: Tungsten rows, not spark.serializer.
reset()
import spark.implicits._
val (_, tDf) = time(trips.toDF().groupBy("zone").count().count())
val dfShuffle = shuffleBytes.get

println(f"$label%-22s rows $cnt%,d | MEMORY_ONLY_SER cache ${serBytes / 1048576.0}%6.1f MiB (${tCache}%.1f s) | " +
        f"RDD shuffle ${rddShuffle / 1048576.0}%6.1f MiB (${tShuffle}%.1f s) | DataFrame shuffle ${dfShuffle / 1024.0}%6.1f KiB")
System.exit(0)
