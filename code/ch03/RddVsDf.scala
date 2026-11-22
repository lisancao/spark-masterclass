// Chapter 3: the same comparison in Scala, so no Python process takes part.
// Run from code/:  uv run spark-shell --master 'local[*]' -i ch03/RddVsDf.scala
import org.apache.spark.sql.functions._
spark.sparkContext.setLogLevel("ERROR")
val path = sys.env("BOOK_DATA") + "/nyc-taxi/yellow"
val trips = spark.read.parquet(path)

def rdd() = trips.select("tpep_pickup_datetime", "fare_amount").rdd
  .filter(r => !r.isNullAt(1) && r.getDouble(1) > 0)
  .map(r => (r.getAs[java.time.LocalDateTime](0).getHour, (r.getDouble(1), 1L)))
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .mapValues(s => (s._2, math.round(s._1 / s._2 * 100) / 100.0))
  .sortByKey().collect()

// The same RDD code without the select: the RDD gets every column of every row.
def rddAll() = trips.rdd
  .filter(r => !r.isNullAt(r.fieldIndex("fare_amount")) && r.getAs[Double]("fare_amount") > 0)
  .map(r => (r.getAs[java.time.LocalDateTime]("tpep_pickup_datetime").getHour,
             (r.getAs[Double]("fare_amount"), 1L)))
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .mapValues(s => (s._2, math.round(s._1 / s._2 * 100) / 100.0))
  .sortByKey().collect()

def df() = trips.where(col("fare_amount") > 0)
  .groupBy(hour(col("tpep_pickup_datetime")).as("hour"))
  .agg(count("*").as("trips"), round(avg("fare_amount"), 2).as("avg_fare"))
  .orderBy("hour").collect()

def median(f: () => Any): Double = {
  f()
  val ts = (1 to 5).map { _ => val t = System.nanoTime; f(); (System.nanoTime - t) / 1e9 }
  ts.sorted.apply(2)
}
val a = median(() => rddAll()); val r = median(() => rdd()); val d = median(() => df())
println(f"scala rdd (all 21 columns)         $a%6.2f s")
println(f"scala rdd (select 2 columns first) $r%6.2f s")
println(f"scala dataframe                    $d%6.2f s   ${r / d}%4.1fx faster than rdd, ${a / d}%4.1fx faster than rdd over all columns")
println("rdd hour 18: " + rdd()(18) + "   dataframe hour 18: " + df()(18))
System.exit(0)
