// Chapter 92: typed Datasets and Encoders in Scala, on Spark Classic.
// Run from code/:  uv run spark-shell --master 'local[*]' --conf spark.ui.port=4157 -I ch92/Datasets.scala
// -I loads the file as one pasted block. With -i, the case classes fail to decode (see notes/v3-apis.md).
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.ui.explainMode", "simple")
val path = sys.env("BOOK_DATA") + "/nyc-taxi/yellow"

// The case class is the schema. Option marks a column that can be null.
case class Trip(PULocationID: Int, payment_type: Option[Long], passenger_count: Option[Long],
                trip_distance: Double, fare_amount: Double, tip_amount: Double)
case class Missing(PULocationID: Int, surge_fee: Double)
case class NoOption(PULocationID: Int, passenger_count: Long)
case class SumCount(sum: Double, count: Long)

def section(s: String): Unit = println(s"\n== $s")
def physical(ds: Dataset[_]): String =
  ds.queryExecution.executedPlan.toString.split("\n").map(_.replaceAll(" Batched: true.*", "")
    .replaceAll("\\$line\\d+\\.\\$read\\$\\$iw\\$\\$iw\\$", "").take(110)).mkString("\n")

section("1. an Encoder is a schema plus code that converts objects to rows")
val enc = Encoders.product[Trip]
println(enc.schema.treeString.trim)
println("Encoders.kryo[Trip]: " + Encoders.kryo[Trip].schema.simpleString)
println("Encoders.scalaInt:   " + Encoders.scalaInt.schema.simpleString)
println("Encoders.STRING:     " + Encoders.STRING.schema.simpleString)

section("2. as[Trip] gives a typed view of the same rows")
val raw = spark.read.parquet(path)
val trips: Dataset[Trip] = raw.as[Trip]
println(s"raw.as[Trip]: ${trips.getClass.getSimpleName}[Trip], columns kept: ${trips.columns.length}")
println("first: " + trips.head())
println("after a typed map, columns: " + trips.map(t => t.fare_amount).columns.mkString(", "))

try raw.as[Missing] catch { case e: org.apache.spark.sql.AnalysisException =>
  println("as[Missing]: " + e.getCondition + " " + e.getMessageParameters) }
try raw.as[NoOption].where($"passenger_count".isNull).head() catch { case e: Exception =>
  val c = Option(e.getCause).getOrElse(e)
  println("as[NoOption] on a null: " + c.getClass.getSimpleName + " " + c.getMessage.split("\n").head.take(150)) }

section("3. a typed filter and map: the plan deserializes and serializes")
val typed = trips.filter(t => t.fare_amount > 0).map(t => (t.PULocationID, t.tip_amount / t.fare_amount))
typed.explain("extended")

section("4. the same logic untyped: plain expressions")
val untyped = raw.where($"fare_amount" > 0).select($"PULocationID", $"tip_amount" / $"fare_amount")
println(physical(untyped))

section("5. typed and untyped, measured (median of 5 after a warmup)")
def median(f: () => Any): Double = {
  f()
  val ts = (1 to 5).map { _ => val t = System.nanoTime(); f(); (System.nanoTime() - t) / 1e9 }
  ts.sorted.apply(2)
}
def noop(ds: Dataset[_]): Unit = ds.write.format("noop").mode("overwrite").save()
val tTyped = median(() => noop(typed))
val tUntyped = median(() => noop(untyped))
val tSelect = median(() => noop(raw.select("PULocationID", "payment_type", "passenger_count", "trip_distance",
  "fare_amount", "tip_amount").as[Trip].filter(t => t.fare_amount > 0).map(t => (t.PULocationID, t.tip_amount / t.fare_amount))))
println(f"typed filter + map      $tTyped%6.2f s")
println(f"untyped where + select  $tUntyped%6.2f s   ${tTyped / tUntyped}%4.1fx faster than typed")
println(f"select first, then typed $tSelect%5.2f s")

section("6. a mixed form: typed Dataset, untyped column functions")
val mixed = trips.where($"fare_amount" > 0).select($"PULocationID".as[Int], ($"tip_amount" / $"fare_amount").as[Double])
println(mixed.getClass.getSimpleName + "[(Int, Double)]")
println(physical(mixed))

section("7. groupByKey and an Aggregator as a typed UDAF")
object TipShare extends Aggregator[Trip, SumCount, Double] {
  def zero: SumCount = SumCount(0.0, 0L)
  def reduce(b: SumCount, t: Trip): SumCount =
    if (t.fare_amount > 0) SumCount(b.sum + t.tip_amount / t.fare_amount, b.count + 1) else b
  def merge(a: SumCount, b: SumCount): SumCount = SumCount(a.sum + b.sum, a.count + b.count)
  def finish(r: SumCount): Double = if (r.count == 0) 0.0 else r.sum / r.count
  def bufferEncoder: Encoder[SumCount] = Encoders.product[SumCount]
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
val byZone = trips.groupByKey(_.PULocationID).agg(TipShare.toColumn.name("tip_share"))
println(byZone.orderBy($"key").filter($"key".isin(132, 138, 161)).collect().map(r => f"${r._1} ${r._2}%.4f").mkString(" | "))
println(physical(byZone))

// The same Aggregator over one column, registered as a SQL function with udaf()
object MeanOf extends Aggregator[Double, SumCount, Double] {
  def zero: SumCount = SumCount(0.0, 0L)
  def reduce(b: SumCount, v: Double): SumCount = SumCount(b.sum + v, b.count + 1)
  def merge(a: SumCount, b: SumCount): SumCount = SumCount(a.sum + b.sum, a.count + b.count)
  def finish(r: SumCount): Double = r.sum / r.count
  def bufferEncoder: Encoder[SumCount] = Encoders.product[SumCount]
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
spark.udf.register("mean_of", udaf(MeanOf))
raw.createOrReplaceTempView("trips")
val q = spark.sql("SELECT PULocationID, mean_of(fare_amount) AS m, avg(fare_amount) AS a FROM trips " +
                  "WHERE PULocationID IN (132, 138) GROUP BY 1 ORDER BY 1")
q.show()
println(physical(q))
val tUdaf = median(() => spark.sql("SELECT PULocationID, mean_of(fare_amount) FROM trips GROUP BY 1").collect())
val tAvg = median(() => spark.sql("SELECT PULocationID, avg(fare_amount) FROM trips GROUP BY 1").collect())
// build a new Dataset for each run: a second collect() of the same Dataset reuses its shuffle files
val tKey = median(() => trips.groupByKey(_.PULocationID).agg(TipShare.toColumn.name("tip_share")).collect())
val tUntypedShare = median(() => raw.groupBy("PULocationID")
  .agg(avg(when($"fare_amount" > 0, $"tip_amount" / $"fare_amount")).as("tip_share")).collect())
println(f"mean_of (Aggregator via udaf)   $tUdaf%5.2f s")
println(f"avg (built-in)                  $tAvg%5.2f s")
println(f"groupByKey + TipShare (typed)   $tKey%5.2f s")
println(f"groupBy + avg(when(...))        $tUntypedShare%5.2f s   (the same tip share, untyped)")
System.exit(0)
