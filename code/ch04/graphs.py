"""Chapter 4: GraphFrames 0.12.3 on Spark 4.3, the DataFrame graph package.

Taxi zones are the vertices, and June trips between zones are the edges. The script needs the
GraphFrames jar and its Python package, which are not part of Spark:
  uv run --with graphframes-py==0.12.3 python ch04/graphs.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, work, TRIPS, ZONES

PKG = "io.graphframes:graphframes-spark4_2.13:0.12.3"
spark = session("ch04-graphs", spark_jars_packages=PKG, spark_ui_showConsoleProgress="false")
spark.sparkContext.setCheckpointDir(str(work("ch04-graphs") / "checkpoints"))

from graphframes import GraphFrame
from importlib.metadata import version
print(f"spark {spark.version}, graphframes-py {version('graphframes-py')}, jar {PKG}")

zones = (spark.read.option("header", True).csv(str(ZONES))
         .select(F.col("LocationID").cast("int").alias("id"), "Borough", "Zone"))
edges = (spark.read.parquet(str(TRIPS))
         .where("PULocationID <> DOLocationID")
         .groupBy(F.col("PULocationID").alias("src"), F.col("DOLocationID").alias("dst"))
         .agg(F.count("*").alias("trips"))
         .where("trips >= 10"))                       # keep the routes with ten trips or more
g = GraphFrame(zones, edges)
print(f"vertices {g.vertices.count()}, edges {g.edges.count()}")

print("\nzones that the most other zones send trips to (inDegrees):")
(g.inDegrees.join(zones, "id").orderBy(F.desc("inDegree"), "id").limit(5)
 .select("id", "Zone", "inDegree").show(truncate=False))

pr = g.pageRank(resetProbability=0.15, maxIter=10)
print("PageRank, 10 iterations, top five zones:")
(pr.vertices.orderBy(F.desc("pagerank")).limit(5)
 .select("id", "Zone", F.round("pagerank", 3).alias("pagerank")).show(truncate=False))

cc = g.connectedComponents()
sizes = cc.groupBy("component").count().orderBy(F.desc("count"))
print("connected components (by size):")
sizes.show(5)
