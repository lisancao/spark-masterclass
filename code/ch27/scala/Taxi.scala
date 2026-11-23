// Chapter 27: the five busiest pickup zones in June 2026, from Spark's Scala Connect client.
//> using scala 2.13.17
//> using dep org.apache.spark::spark-connect-client-jvm:4.3.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Taxi {
  def main(args: Array[String]): Unit = {
    val remote = sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15106")
    val t0 = System.nanoTime()
    val spark = SparkSession.builder().remote(remote).getOrCreate()
    println(s"spark-connect-client-jvm 4.3.0 -> server ${spark.version} at $remote")
    val pickup = col("tpep_pickup_datetime")
    val top = spark.read.parquet(sys.env("TRIPS"))
      .where(pickup >= "2026-06-01" && pickup < "2026-07-01")
      .groupBy("PULocationID")
      .agg(count(lit(1)).as("trips"), round(avg("fare_amount"), 2).as("avg_fare"))
      .orderBy(desc("trips"))
      .limit(5)
    top.show()
    spark.stop()
    println(f"wall time ${(System.nanoTime() - t0) / 1e9}%.2f s")
  }
}
