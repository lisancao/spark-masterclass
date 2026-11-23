// Chapter 21: the five busiest pickup zones in June 2026, from the Swift client.
import Foundation
import SparkConnect

let env = ProcessInfo.processInfo.environment
let remote = env["SPARK_REMOTE"] ?? "sc://localhost:15106"
let start = Date()
let spark = try await SparkSession.builder.remote(remote).getOrCreate()
print("spark-connect-swift 0.8.0 -> server \(await spark.version) at \(remote)")

let pickup = col("tpep_pickup_datetime")
let top = await spark.read.parquet(env["TRIPS"]!)
  .filter(pickup >= lit("2026-06-01") && pickup < lit("2026-07-01"))
  .groupBy("PULocationID")
  .agg(count(lit(1)).alias("trips"), round(avg(col("fare_amount")), 2).alias("avg_fare"))
  .orderBy(col("trips").desc())
  .limit(5)
try await top.show()
await spark.stop()
print(String(format: "wall time %.2f s", Date().timeIntervalSince(start)))
