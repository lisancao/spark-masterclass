//! Chapter 22: where does local work stop paying? For pickup windows from one day to six months:
//!   spark: send the aggregation as a plan, get the small result back as Arrow (one question)
//!   pull:  get the raw rows of the window as Arrow into Polars (once)
//!   local: the same aggregation in Polars over the pulled rows (each further question)
//! Each number is the median of 3 runs after a warmup run. Run it under the bench lock.
use std::time::Instant;

use polars::prelude::*;
use taxi_app::{connect, ms, raw_rows, to_polars, zone_hours};

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn local_agg(raw: &DataFrame) -> PolarsResult<DataFrame> {
    raw.clone()
        .lazy()
        .group_by([col("PULocationID"), col("tpep_pickup_datetime").dt().hour().alias("hour")])
        .agg([len().alias("trips"), col("fare_amount").mean().round(2, RoundMode::HalfToEven).alias("avg_fare")])
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = connect()?;
    let ends = [
        ("1 day", "2026-01-02"), ("3 days", "2026-01-04"), ("1 week", "2026-01-08"),
        ("2 weeks", "2026-01-15"), ("1 month", "2026-02-01"), ("2 months", "2026-03-01"),
        ("3 months", "2026-04-01"), ("6 months", "2026-07-01"),
    ];
    println!("{:<9} {:>10} {:>9} {:>9} {:>9} {:>9} {:>11}", "window", "rows", "pull MiB", "spark ms", "pull ms", "local ms", "break-even");
    for (label, end) in ends {
        let (mut sp, mut pu, mut lo) = (vec![], vec![], vec![]);
        let (mut rows, mut mib, mut groups) = (0, 0.0, (0, 0));
        for run in 0..4 {
            let t = Instant::now();
            let (agg, _) = to_polars(&zone_hours(&spark, "2026-01-01", end))?;
            let s = ms(t);
            let t = Instant::now();
            let (raw, bytes) = to_polars(&raw_rows(&spark, "2026-01-01", end))?;
            let p = ms(t);
            let t = Instant::now();
            let local = local_agg(&raw)?;
            let l = ms(t);
            if run > 0 {
                sp.push(s); pu.push(p); lo.push(l);
            }
            rows = raw.height();
            mib = bytes as f64 / 1048576.0;
            groups = (agg.height(), local.height());
        }
        assert_eq!(groups.0, groups.1, "Spark and Polars must find the same groups");
        let (s, p, l) = (median(sp), median(pu), median(lo));
        // After the pull, each question costs `l` locally instead of `s` on Spark.
        let even = if s > l { format!("{:.1} q", p / (s - l)) } else { "never".into() };
        println!("{label:<9} {rows:>10} {mib:>9.1} {s:>9.0} {p:>9.0} {l:>9.1} {even:>11}");
    }
    Ok(())
}
