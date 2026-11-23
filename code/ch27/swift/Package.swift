// swift-tools-version: 6.0
// Chapter 27: the Swift client from its 0.8.0 release tag.
import PackageDescription

let package = Package(
  name: "Taxi",
  platforms: [.macOS(.v15)],
  dependencies: [
    .package(url: "https://github.com/apache/spark-connect-swift.git", exact: "0.8.0")
  ],
  targets: [
    .executableTarget(
      name: "Taxi",
      dependencies: [.product(name: "SparkConnect", package: "spark-connect-swift")]),
    .executableTarget(
      name: "Probe",
      dependencies: [.product(name: "SparkConnect", package: "spark-connect-swift")]),
  ]
)
