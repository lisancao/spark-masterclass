// Chapter 21: the same capability probes in every client. Each line: case, result, detail.
import Foundation
import SparkConnect

func unwrap(_ x: Any) -> Any {  // Row values arrive boxed as Optional
  let m = Mirror(reflecting: x)
  return m.displayStyle == .optional ? m.children.first.map { unwrap($0.value) } ?? "null" : x
}
func show(_ r: Row) -> String { "[" + (0..<r.length).map { "\(unwrap((try? r.get($0)) as Any))" }.joined(separator: ", ") + "]" }
func pad(_ s: String) -> String { s.padding(toLength: 12, withPad: " ", startingAt: 0) }

func report(_ name: String, _ body: () async throws -> String) async {
  do {
    print("\(pad(name)) works   \(try await body())")
  } catch {
    print("\(pad(name)) breaks  \(String(describing: error).split(separator: "\n").first ?? "")")
  }
}

let env = ProcessInfo.processInfo.environment
let remote = env["SPARK_REMOTE"] ?? "sc://localhost:15106"
let trips = env["TRIPS"]!, work = env["WORK"]!
let spark = try await SparkSession.builder.remote(remote).getOrCreate()
print("swift (spark-connect-swift 0.8.0) -> server \(await spark.version)")

await report("sql") { show(try await spark.sql("SELECT 1 AS x, 'a' AS y").collect()[0]) }
await report("schema") {
  let cols = try await spark.read.parquet(trips).columns
  return "\(cols.count) columns, first \(cols[0])"
}
await report("local_data") {
  let df = try await spark.createDataFrame([[Int64(1), "Toronto"], [Int64(2), "Hanoi"]], "id BIGINT, city STRING")
  return "count \(try await df.count())"
}
await report("temp_view") {
  try await spark.range(5).createOrReplaceTempView("five")
  return show(try await spark.sql("SELECT count(*) AS n FROM five").collect()[0])
}
await report("write_read") {
  try await spark.range(10).write.mode("overwrite").parquet(work + "/swift")
  return "read back \(try await spark.read.parquet(work + "/swift").count()) rows"
}
await report("config") {
  try await spark.conf.set("spark.sql.shuffle.partitions", "7")
  return "\"\(try await spark.conf.get("spark.sql.shuffle.partitions"))\""
}
await report("error") {
  do {
    _ = try await spark.read.parquet(trips).select("revenue").collect()
    return "no error"
  } catch {
    let s = String(describing: error)
    return "\(s.contains("UNRESOLVED_COLUMN") ? "UNRESOLVED_COLUMN" : "?"), SQLSTATE \(s.contains("42703") ? "42703" : "?")"
  }
}
print("\(pad("arrow")) n/o     collect() returns Rows; the Arrow reader is internal")
await report("explain") {
  try await spark.range(3).explain("simple")   // prints the plan, returns nothing
  return "the plan above"
}
print("\(pad("zip")) n/o     DataFrame has no zip method")
await report("big_result") { "\(try await spark.sql("SELECT id, repeat('x', 100) AS pad FROM range(200000)").collect().count) rows" }
await spark.stop()
