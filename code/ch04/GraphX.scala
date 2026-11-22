// Chapter 4: the same zone graph in GraphX, the RDD-based graph library inside Spark (Scala and Java only).
// Run from code/:  uv run spark-shell --master 'local[*]' -i ch04/GraphX.scala
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
spark.sparkContext.setLogLevel("ERROR")
val trips = spark.read.parquet(sys.env("BOOK_DATA") + "/nyc-taxi/yellow")
val edges: org.apache.spark.rdd.RDD[Edge[Long]] = trips
  .where("PULocationID <> DOLocationID")
  .groupBy("PULocationID", "DOLocationID").count().where("count >= 10")
  .rdd.map(r => Edge(r.getInt(0).toLong, r.getInt(1).toLong, r.getLong(2)))
val graph = Graph.fromEdges(edges, defaultValue = 0)
println(s"GraphX: vertices ${graph.numVertices}, edges ${graph.numEdges}")
val ranks = graph.staticPageRank(numIter = 10, resetProb = 0.15).vertices
println("staticPageRank, 10 iterations, top five vertex ids: " +
  ranks.sortBy(-_._2).take(5).map { case (id, r) => f"$id ($r%.3f)" }.mkString(", "))
val cc = graph.connectedComponents().vertices.map(_._2).countByValue()
println(s"connected components: ${cc.size}, largest has ${cc.values.max} vertices")
System.exit(0)
