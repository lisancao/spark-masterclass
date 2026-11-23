// Chapter 92: the same typed Dataset code over Spark Connect, in the Scala Connect REPL.
// Server: ch90/connect_server.sh start. Run from code/ (the REPL needs a terminal, so script(1) gives it one):
//   ch92/connect_repl.sh
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
case class Trip(PULocationID: Int, payment_type: Option[Long], passenger_count: Option[Long], trip_distance: Double, fare_amount: Double, tip_amount: Double)
case class SumCount(sum: Double, count: Long)
class TipShareAgg extends Aggregator[Trip, SumCount, Double] { def zero = SumCount(0.0, 0L); def reduce(b: SumCount, t: Trip) = if (t.fare_amount > 0) SumCount(b.sum + t.tip_amount / t.fare_amount, b.count + 1) else b; def merge(a: SumCount, b: SumCount) = SumCount(a.sum + b.sum, a.count + b.count); def finish(r: SumCount) = if (r.count == 0) 0.0 else r.sum / r.count; def bufferEncoder: Encoder[SumCount] = Encoders.product[SumCount]; def outputEncoder: Encoder[Double] = Encoders.scalaDouble }
val trips = spark.read.parquet(sys.env("BOOK_DATA") + "/nyc-taxi/yellow").as[Trip]
println("CLASS " + trips.getClass.getName)
println("FIRST " + trips.head())
val typed = trips.filter(t => t.fare_amount > 0).map(t => (t.PULocationID, t.tip_amount / t.fare_amount))
println("COUNT " + typed.count())
typed.explain()
println("GBK_COUNT " + trips.groupByKey(_.PULocationID).count().collect().length)
val TipShare = new TipShareAgg
try println("SHARE " + trips.groupByKey(_.PULocationID).agg(TipShare.toColumn.name("tip_share")).collect().length) catch { case e: Exception => println("SHARE failed: " + e.getMessage.take(110)) }
