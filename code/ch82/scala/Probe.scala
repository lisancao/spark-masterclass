// Chapter 82: the same capability probes in every client, here in the Scala client.
//> using scala 2.13.17
//> using dep org.apache.spark::spark-connect-client-jvm:4.3.0
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Probe {
  def report(name: String)(body: => String): Unit =
    try println(f"$name%-12s works   ${body}")
    catch { case e: Exception => println(f"$name%-12s breaks  ${e.getMessage.linesIterator.next()}") }

  def main(args: Array[String]): Unit = {
    val trips = sys.env("TRIPS"); val work = sys.env("WORK")
    val spark = SparkSession.builder().remote(sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15106")).getOrCreate()
    import spark.implicits._
    println(s"scala (spark-connect-client-jvm 4.3.0) -> server ${spark.version}")
    report("sql") { spark.sql("SELECT 1 AS x, 'a' AS y").collect()(0).toSeq.mkString("[", ", ", "]") }
    report("schema") { val c = spark.read.parquet(trips).columns; s"${c.length} columns, first ${c(0)}" }
    report("local_data") { s"count ${Seq((1L, "Toronto"), (2L, "Hanoi")).toDF("id", "city").count()}" }
    report("temp_view") {
      spark.range(5).createOrReplaceTempView("five")
      spark.sql("SELECT count(*) AS n FROM five").collect()(0).toSeq.mkString("[", ", ", "]")
    }
    report("write_read") {
      spark.range(10).write.mode("overwrite").parquet(s"$work/scala")
      s"read back ${spark.read.parquet(s"$work/scala").count()} rows"
    }
    report("config") { spark.conf.set("spark.sql.shuffle.partitions", "7"); "\"" + spark.conf.get("spark.sql.shuffle.partitions") + "\"" }
    report("error") {
      try { spark.read.parquet(trips).select("revenue").collect(); "no error" }
      catch { case e: org.apache.spark.sql.AnalysisException => s"${e.getCondition.split('.')(0)}, SQLSTATE ${e.getSqlState}" }
    }
    println(f"${"arrow"}%-12s n/o     no public API returns the Arrow batches")
    report("explain") {
      val out = new java.io.ByteArrayOutputStream()
      Console.withOut(out) { spark.range(3).explain() }
      out.toString.linesIterator.find(_.contains("Range")).get.trim
    }
    report("zip") { s"count ${spark.range(3).zip(spark.range(3).withColumnRenamed("id", "id2")).count()}" }
    report("big_result") { s"${spark.sql("SELECT id, repeat('x', 100) AS pad FROM range(200000)").collect().length} rows" }
    spark.stop()
  }
}
