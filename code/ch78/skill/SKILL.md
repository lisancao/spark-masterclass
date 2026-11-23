---
name: spark-4-3-pipelines
description: Write Spark Declarative Pipelines and DataFrame code for open source Apache Spark 4.3.0. Use for any pipeline, transform or query that will run on this team's Spark 4.3 Connect server.
---

# Spark 4.3 pipelines for this team

Pinned to: Apache Spark 4.3.0, pyspark 4.3.0, Python 3.12 on the server image. Each rule below
was checked on 4.3.0 in Spark Masterclass Chapter 78 (code/ch78/output/).

## Declare, do not run

- Import `from pyspark import pipelines as dp`. There is no `dlt` module in open source Spark.
- One flow per function, `@dp.materialized_view` or `@dp.table`. Return a DataFrame. No actions
  (`collect`, `count`, `toPandas`) and no Python loops over rows inside a flow.
- Never call `spark.conf.set` inside a flow. Put configs in `spark-pipeline.yml` under `configuration`.
- Run `spark-pipelines dry-run` before `spark-pipelines run`. Fix every error it reports.

## Version facts

- ANSI mode is on. `cast` raises `CAST_INVALID_INPUT` on a bad value. Use `try_cast` for input
  that can be dirty, then count the nulls it made.
- Python UDFs over Connect: give `useArrow` explicitly. Prefer built-in functions to UDFs.
- Epoch milliseconds: `F.timestamp_millis(col)`, never `col.cast("timestamp")`.

## Time

- Day boundaries are UTC: `F.to_date(F.to_utc_timestamp(F.to_timestamp(col), F.current_timezone()))`.

## Metrics

- Do not compute business metrics by hand. Use the metric views in the catalog:
  `SELECT dim, MEASURE(metric) FROM <view> GROUP BY dim`. List them with `SHOW VIEWS`.
  Ask before you average a ratio or sum a distinct count.

## Scope

- Write only to the namespace `sandbox_<agent id>`. Read other namespaces. Never write elsewhere.
