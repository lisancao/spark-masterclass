// Step 9: mapGroupsWithState and flatMapGroupsWithState, the Scala and Java APIs before transformWithState.
// The spend per user of Step 1, with one GroupState[Double] per user.
// Run from code/:  ch41/t7_groupstate.sh
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import spark.implicits._

// Tuples, not case classes: a case class defined in the shell needs the shell's outer object,
// and the executor thread waits for it while the driver thread runs the query.
type Pay = (String, String, Double)          // (user, event_id, amount)
def r2(x: Double) = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

spark.sparkContext.setLogLevel("ERROR")
val inbox = sys.env("INBOX")
val work = sys.env("WORK")
val pays = spark.readStream.schema("event_id STRING, user STRING, action STRING, page STRING, ts TIMESTAMP, amount DOUBLE")
  .option("maxFilesPerTrigger", 1).json(inbox)
  .where("action = 'pay'").select("user", "event_id", "amount").as[(String, String, Double)]

// mapGroupsWithState: exactly one output row per key that has new rows, update mode only.
def spend(user: String, rows: Iterator[Pay], state: GroupState[Double]): (String, Double, Int) = {
  val ps = rows.toSeq
  val total = state.getOption.getOrElse(0.0) + ps.map(_._3).sum
  state.update(total)
  (user, r2(total), ps.size)                  // (user, total, payments)
}
val q1 = pays.groupByKey(_._1).mapGroupsWithState(GroupStateTimeout.NoTimeout)(spend _)
  .writeStream.format("memory").queryName("spend").outputMode("update")
  .option("checkpointLocation", s"$work/ck_map").trigger(Trigger.AvailableNow()).start()
q1.awaitTermination()
println("1. mapGroupsWithState, update mode: one row per user per batch with payments")
spark.table("spend").toDF("user", "total", "payments").show()

// flatMapGroupsWithState: zero or more rows per key, here one row only when a user passes 50.00.
def alerts(user: String, rows: Iterator[Pay], state: GroupState[Double]): Iterator[(String, Double, String)] = {
  val before = state.getOption.getOrElse(0.0)
  var total = before
  var out = List.empty[(String, Double, String)]
  for (p <- rows) {
    total += p._3
    if (before <= 50.0 && total > 50.0 && out.isEmpty) out = List((user, r2(total), p._2))   // (user, total, crossed_by)
  }
  state.update(total)
  out.iterator
}
val q2 = pays.groupByKey(_._1)
  .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout)(alerts _)
  .writeStream.format("memory").queryName("alerts").outputMode("append")
  .option("checkpointLocation", s"$work/ck_flat").trigger(Trigger.AvailableNow()).start()
q2.awaitTermination()
println("2. flatMapGroupsWithState, append mode: a row only when a user passes 50.00")
spark.table("alerts").toDF("user", "total", "crossed_by").show()
println("   batches (id, input rows): " + q2.recentProgress.map(p => (p.batchId, p.numInputRows)).mkString(", "))
println("   state operator: " + q2.lastProgress.stateOperators(0).operatorName)

println("3. mapGroupsWithState in append mode:")
try {
  pays.groupByKey(_._1).mapGroupsWithState(GroupStateTimeout.NoTimeout)(spend _)
    .writeStream.format("memory").queryName("x").outputMode("append")
    .option("checkpointLocation", s"$work/ck_bad").start().stop()
  println("   accepted")
} catch { case e: Exception => println("   " + e.getClass.getSimpleName + ": " + e.getMessage.split("\n")(0).take(150)) }
System.exit(0)
