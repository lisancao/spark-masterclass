//! The parts of the taxi app that both binaries use: the session, the Arrow hand-offs and the queries.
use std::io::Cursor;
use std::time::Instant;

use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use polars::prelude::{DataFrame as PlDataFrame, IpcReader, SerReader};
use spark_connect::functions::{avg, count, hour, round_scale};
use spark_connect::{col, lit, lit_string, DataFrame, SparkSession};

/// Connection settings come from the environment, so the same binary runs against any server.
/// SPARK_REMOTE can carry `;user_id=...;token=...;use_ssl=true` parameters.
pub fn connect() -> spark_connect::Result<SparkSession> {
    let remote = std::env::var("SPARK_REMOTE").unwrap_or_else(|_| "sc://localhost:15106/;user_id=taxi-app".into());
    let spark = SparkSession::builder().remote(&remote).get_or_create()?;
    // Session settings that this app depends on. They live in the server session, not in the binary.
    spark.conf().set("spark.sql.shuffle.partitions", "16")?;
    spark.conf().set("spark.sql.session.timeZone", "UTC")?;
    // Every operation of this process carries the tag: the server UI, the REST API and
    // spark.interrupt_tag() can all find them by it.
    spark.add_tag("taxi-app")?;
    Ok(spark)
}

/// The trips folder as the *server* sees it.
pub fn trips_path() -> String {
    std::env::var("TRIPS").expect("TRIPS = the taxi folder, as the server sees it")
}

/// The heavy query: trips and average fare per pickup zone and hour, over a pickup window.
/// The client only builds the plan; Spark reads the Parquet files and does the aggregation.
pub fn zone_hours(spark: &SparkSession, from: &str, to: &str) -> DataFrame {
    let pickup = || col("tpep_pickup_datetime");
    spark
        .read()
        .parquet(&trips_path())
        .filter(pickup().ge(lit_string(from)).and(pickup().lt(lit_string(to))))
        .group_by([col("PULocationID"), hour(pickup()).alias("hour")])
        .agg([count(lit(1)).alias("trips"), round_scale(avg(col("fare_amount")), lit(2)).alias("avg_fare")])
}

/// The raw rows of the same window, three columns: what the app pulls when it works locally.
pub fn raw_rows(spark: &SparkSession, from: &str, to: &str) -> DataFrame {
    let pickup = || col("tpep_pickup_datetime");
    spark
        .read()
        .parquet(&trips_path())
        .filter(pickup().ge(lit_string(from)).and(pickup().lt(lit_string(to))))
        .select([col("PULocationID"), pickup(), col("fare_amount")])
}

/// Spark -> Polars. `to_arrow()` returns the result as Arrow IPC (file format) bytes, and
/// Polars reads the buffers from them. No value goes through a Rust row type.
pub fn to_polars(df: &DataFrame) -> Result<(PlDataFrame, usize), Box<dyn std::error::Error>> {
    let ipc = df.to_arrow()?;
    let n = ipc.len();
    Ok((IpcReader::new(Cursor::new(ipc)).finish()?, n))
}

/// Arrow IPC bytes -> arrow-rs 60 RecordBatches, the type that DuckDB 2.0 takes.
pub fn ipc_to_batches(ipc: &[u8]) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    Ok(FileReader::try_new(Cursor::new(ipc.to_vec()), None)?.collect::<Result<_, _>>()?)
}

pub fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}
