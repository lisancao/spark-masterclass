//! Chapter 82: the five busiest pickup zones in June 2026, from the Rust client.
use spark_connect::functions::{avg, count, round_scale};
use spark_connect::{col, lit, lit_string, SparkSession};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let remote = std::env::var("SPARK_REMOTE").unwrap_or_else(|_| "sc://localhost:15106".into());
    let trips_path = std::env::var("TRIPS").expect("TRIPS = the taxi folder, as the server sees it");
    let t0 = std::time::Instant::now();
    let spark = SparkSession::builder().remote(&remote).get_or_create()?;
    println!("apache-spark-connect 4.3.0 -> server {} at {remote}", spark.version()?);

    let pickup = || col("tpep_pickup_datetime");
    let top = spark
        .read()
        .parquet(&trips_path)
        .filter(pickup().ge(lit_string("2026-06-01"))
            .and(pickup().lt(lit_string("2026-07-01"))))
        .group_by([col("PULocationID")])
        .agg([
            count(lit(1)).alias("trips"),
            round_scale(avg(col("fare_amount")), lit(2)).alias("avg_fare"),
        ])
        .order_by([col("trips").desc()])
        .limit(5);
    top.show(5)?;
    println!("wall time {:.2} s", t0.elapsed().as_secs_f64());
    Ok(())
}
