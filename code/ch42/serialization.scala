// Chapter 42: what spark.serializer changes. Run through ch42/serialization.sh (spark-shell -i).
// One run = one serializer, because the serializer is fixed when the SparkContext starts.
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel
import java.util.concurrent.atomic.AtomicLong
import book.Trip                     // a plain Java class from ch42/trip.jar (ch42/java/book/Trip.java)

val label = sys.env.getOrElse("SER_LABEL", "?")
val n = sys.env.getOrElse("SER_ROWS", "2000000").toInt
spark.sparkContext.setLogLevel("ERROR")

// Count the shuffle bytes of the tasks with a listener.
val shuffleBytes = new AtomicLong(0)
sc.addSparkListener(new SparkListener {
  override def onTaskEnd(e: SparkListenerTaskEnd): Unit = if (e.taskMetrics != null)
    shuffleBytes.addAndGet(e.taskMetrics.shuffleWriteMetrics.bytesWritten)
})
def settle(): Unit = Thread.sleep(500)
// Median of 3 runs after one warmup.
def median(f: => Unit): Double = {
  f
  val ts = (1 to 3).map { _ => val t0 = System.nanoTime; f; (System.nanoTime - t0) / 1e9 }.sorted
  ts(1)
}
def failed(e: Throwable): Nothing = {
  val root = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toList.last
  println(f"$label%-27s FAILED: ${root.getClass.getSimpleName}: ${root.getMessage.take(150)}"); System.exit(0); ???
}

val vendors = Array("CMT", "VTS", "DDS", "MYC")
def trips = sc.parallelize(0 until n, 16).map(i => new Trip(i.toLong, i % 265, (i % 1000) / 10.0, vendors(i % 4)))

// 1. Objects in the heap: a deserialized cache (no serializer).
val de = trips.persist(StorageLevel.MEMORY_ONLY); de.count()
val deBytes = sc.getRDDStorageInfo.find(_.id == de.id).map(_.memSize).getOrElse(0L); de.unpersist(true)

// 2. A serialized cache: one byte array per partition, written by spark.serializer.
val ser = trips.persist(StorageLevel.MEMORY_ONLY_SER)
try ser.count() catch { case e: Exception => failed(e) }
val serBytes = sc.getRDDStorageInfo.find(_.id == ser.id).map(_.memSize).getOrElse(0L); ser.unpersist(true)
val tSer = median { val r = trips.persist(StorageLevel.MEMORY_ONLY_SER); r.count(); r.unpersist(true) }

// 3. An RDD shuffle of the objects: the map output is written by spark.serializer (and compressed).
settle(); shuffleBytes.set(0)
trips.map(t => (t.zone, t)).groupByKey(16).mapValues(_.size).count(); settle()
val rddShuffle = shuffleBytes.get
val tShuffle = median { trips.map(t => (t.zone, t)).groupByKey(16).mapValues(_.size).count() }

// 4. The same rows as a DataFrame aggregate: Tungsten rows, not spark.serializer.
import spark.implicits._
settle(); shuffleBytes.set(0)
trips.map(t => (t.id, t.zone, t.distance, t.vendor)).toDF("id", "zone", "distance", "vendor").groupBy("zone").count().count()
settle()
val dfShuffle = shuffleBytes.get

println(f"$label%-27s objects ${deBytes / 1048576.0}%6.1f MiB | MEMORY_ONLY_SER ${serBytes / 1048576.0}%5.1f MiB $tSer%4.2f s | " +
        f"RDD shuffle ${rddShuffle / 1048576.0}%5.1f MiB $tShuffle%4.2f s | DataFrame shuffle ${dfShuffle / 1024.0}%5.1f KiB")
System.exit(0)
