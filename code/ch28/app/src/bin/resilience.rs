//! Chapter 28: the app against a server that cuts streams or is not there yet.
//!   resilience pull     pull one month of raw rows (71 MiB of Arrow); run it against `server.sh small-streams`
//!   resilience wait     run one query; start it while the server is down, then start the server
use std::time::Instant;

use taxi_app::{connect, raw_rows, to_polars};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "pull".into());
    let t = Instant::now();
    let spark = connect()?;
    println!("[{:5.1} s] session {}", t.elapsed().as_secs_f64(), spark.session_id());
    if mode == "pull" {
        let (df, bytes) = to_polars(&raw_rows(&spark, "2026-01-01", "2026-02-01"))?;
        println!("[{:5.1} s] {} rows, {:.1} MiB of Arrow IPC", t.elapsed().as_secs_f64(), df.height(), bytes as f64 / 1048576.0);
    } else {
        let n = spark.range(1000)?.count()?;
        println!("[{:5.1} s] query done, count() = {n}", t.elapsed().as_secs_f64());
    }
    Ok(())
}
