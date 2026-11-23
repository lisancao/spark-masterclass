//! Chapter 21: the same capability probes in every client. Each line: case, result, detail.
use spark_connect::row::{Row, Value};
use spark_connect::types::DataType;
use spark_connect::{col, SparkSession};

type R = Result<String, Box<dyn std::error::Error>>;

fn report(case: &str, r: R) {
    match r {
        Ok(d) => println!("{case:<12} works   {d}"),
        Err(e) => println!("{case:<12} breaks  {}", e.to_string().lines().next().unwrap_or("")),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let remote = std::env::var("SPARK_REMOTE").unwrap_or_else(|_| "sc://localhost:15106".into());
    let trips = std::env::var("TRIPS")?;
    let work = std::env::var("WORK")?;
    let spark = SparkSession::builder().remote(&remote).get_or_create()?;
    println!("rust (apache-spark-connect 4.3.0) -> server {}", spark.version()?);

    report("sql", (|| -> R {
        let rows = spark.sql("SELECT 1 AS x, 'a' AS y")?.collect()?;
        Ok(format!("{}", rows[0]))
    })());
    report("schema", (|| -> R {
        let cols = spark.read().parquet(&trips).columns()?;
        Ok(format!("{} columns, first {}", cols.len(), cols[0]))
    })());
    report("local_data", (|| -> R {
        let schema = DataType::from_ddl("struct<id:bigint,city:string>")?;
        let f = vec!["id".to_string(), "city".to_string()];
        let rows = vec![
            Row::new(f.clone(), vec![Value::Long(1), Value::String("Toronto".into())]),
            Row::new(f, vec![Value::Long(2), Value::String("Hanoi".into())]),
        ];
        Ok(format!("count {}", spark.create_dataframe(rows, schema)?.count()?))
    })());
    report("temp_view", (|| -> R {
        spark.range(5)?.create_or_replace_temp_view("five")?;
        Ok(format!("{}", spark.sql("SELECT count(*) AS n FROM five")?.collect()?[0]))
    })());
    report("write_read", (|| -> R {
        let path = format!("{work}/rust");
        spark.range(10)?.write().mode("overwrite").parquet(&path)?;
        Ok(format!("read back {} rows", spark.read().parquet(&path).count()?))
    })());
    report("config", (|| -> R {
        spark.conf().set("spark.sql.shuffle.partitions", "7")?;
        Ok(format!("{:?}", spark.conf().get("spark.sql.shuffle.partitions")?))
    })());
    report("error", (|| -> R {
        match spark.read().parquet(&trips).select([col("revenue")]).collect() {
            Ok(_) => Err("no error".into()),
            Err(e) => {
                let s = e.to_string();
                let class = if s.contains("UNRESOLVED_COLUMN") { "UNRESOLVED_COLUMN" } else { "?" };
                Ok(format!("{class}, SQLSTATE {}", if s.contains("42703") { "42703" } else { "?" }))
            }
        }
    })());
    report("arrow", (|| -> R {
        let batches = spark.range(1000)?.collect_record_batches()?;
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        Ok(format!("{} RecordBatch(es), {n} rows", batches.len()))
    })());
    report("explain", (|| -> R {
        let plan = spark.range(3)?.explain_string("simple")?;
        Ok(plan.lines().find(|l| l.contains("Range")).unwrap_or("?").trim().to_string())
    })());
    report("zip", (|| -> R {
        let a = spark.range(3)?;
        let b = spark.range(3)?.select([col("id").alias("id2")]);
        Ok(format!("count {}", a.zip(&b)?.count()?))
    })());
    report("big_result", (|| -> R {
        let rows = spark.sql("SELECT id, repeat('x', 100) AS pad FROM range(200000)")?.collect()?;
        Ok(format!("{} rows", rows.len()))
    })());
    Ok(())
}
