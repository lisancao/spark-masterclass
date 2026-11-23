//! Chapter 22: a Rust application on the Spark Connect client.
//! Small and interactive work stays in the process (Polars, DuckDB). Heavy work goes to Spark as a
//! plan, and the result comes back as Arrow batches that Polars and DuckDB read as they are.
use std::time::Instant;

use arrow::util::pretty::pretty_format_batches;
use duckdb::vtab::arrow::{ArrowBatchRegistration, ArrowVTab};
use duckdb::Connection;
use polars::prelude::*;
use spark_connect::col;
use taxi_app::{connect, ipc_to_batches, ms, to_polars, zone_hours};

fn section(t: &str) {
    println!("\n== {t}");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ------------------------------------------------------------------ 1. session
    section("1. Connect, and set the session up from the app");
    let spark = connect()?;
    println!("server {} | session {}", spark.version()?, spark.session_id());
    println!("spark.sql.shuffle.partitions = {:?}", spark.conf().get("spark.sql.shuffle.partitions")?);

    // ------------------------------------------------------------------ 2. local only
    section("2. Local: the zone lookup in Polars (Spark is not involved)");
    let zones_csv = std::env::var("ZONES").expect("ZONES = taxi_zone_lookup.csv, as the app sees it");
    let t = Instant::now();
    let zones = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(zones_csv.into()))?
        .finish()?
        .lazy()
        .select([col_pl("LocationID").cast(DataType::Int32), col_pl("Borough"), col_pl("Zone")])
        .collect()?;
    println!("{} zones read in {:.1} ms", zones.height(), ms(t));

    // ------------------------------------------------------------------ 3. heavy, on Spark
    section("3. Heavy: six months of trips, aggregated by Spark, back as Arrow");
    let plan = zone_hours(&spark, "2026-01-01", "2026-07-01");
    let t = Instant::now();
    let (hours, ipc_bytes) = to_polars(&plan)?;
    println!("Spark -> Arrow IPC ({:.1} KiB) -> Polars: {} rows x {} columns in {:.0} ms",
        ipc_bytes as f64 / 1024.0, hours.height(), hours.width(), ms(t));
    println!("schema: {:?}", hours.schema());

    // ------------------------------------------------------------------ 4. local follow-ups
    section("4. Interactive: follow-up questions answered in Polars, no round trip");
    let t = Instant::now();
    let by_borough = hours
        .clone()
        .lazy()
        .join(zones.clone().lazy(), [col_pl("PULocationID")], [col_pl("LocationID")], JoinArgs::new(JoinType::Left))
        .group_by([col_pl("Borough")])
        .agg([col_pl("trips").sum().alias("trips")])
        .with_columns([(col_pl("trips").cast(DataType::Float64) * lit_pl(100.0) / col_pl("trips").sum())
            .round(1, RoundMode::HalfToEven)
            .alias("pct")])
        .sort(["trips"], SortMultipleOptions::default().with_order_descending(true))
        .collect()?;
    println!("{by_borough}");
    println!("question 1 in {:.1} ms", ms(t));
    let t = Instant::now();
    let peak = hours
        .clone()
        .lazy()
        .group_by([col_pl("hour")])
        .agg([col_pl("trips").sum()])
        .sort(["trips"], SortMultipleOptions::default().with_order_descending(true))
        .limit(3)
        .collect()?;
    println!("{peak}");
    println!("question 2 in {:.1} ms", ms(t));

    // ------------------------------------------------------------------ 5. DuckDB on the same Arrow
    section("5. DuckDB SQL over the same Arrow result (registered, not copied)");
    let batches = ipc_to_batches(&plan.to_arrow()?)?;
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    // The registration keeps the batch alive while the view uses it. DuckDB scans the Arrow buffers.
    let reg = ArrowBatchRegistration::new(batch);
    db.execute("SET VARIABLE zone_hours_batch = ?", [&reg])?;
    db.execute_batch("CREATE VIEW zone_hours AS SELECT * FROM arrow(getvariable('zone_hours_batch'))")?;
    let mut stmt = db.prepare(
        "SELECT PULocationID, hour, trips, avg_fare
         FROM zone_hours
         QUALIFY row_number() OVER (PARTITION BY hour ORDER BY trips DESC) = 1
         ORDER BY trips DESC LIMIT 5",
    )?;
    let top: Vec<_> = stmt.query_arrow([])?.collect();
    println!("busiest zone of each hour, top 5:\n{}", pretty_format_batches(&top)?);

    // ------------------------------------------------------------------ 6. errors
    section("6. Errors: what the app gets when the server rejects a plan");
    match plan.select([col("revenue")]).count() {
        Ok(_) => println!("no error?"),
        Err(e) => {
            println!("condition: {:?}", e.get_condition());
            println!("sqlstate:  {:?}", e.get_sql_state());
            println!("operation: {:?}", e.operation_id);
            println!("message:   {}", e.to_string().lines().next().unwrap_or("").chars().take(88).collect::<String>());
        }
    }
    Ok(())
}

fn col_pl(name: &str) -> Expr {
    polars::prelude::col(name)
}

fn lit_pl(v: f64) -> Expr {
    polars::prelude::lit(v)
}
