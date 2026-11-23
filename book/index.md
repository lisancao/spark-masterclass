# Spark Masterclass

**An open, in-depth book about Apache Spark 4.3. Each example runs on your laptop, and each number comes from a run that you can do again.**

Most Spark material stops at the API. Other material assumes that a managed platform does the important parts, behind a paywall. This book does the opposite. It starts from a `pip install`, and it uses one month of real New York taxi trips. At each step, it asks what Spark did. It examines the plans that Spark built, the jobs and tasks that Spark ran, the files that Spark wrote and the bytes that Spark moved.

The book is for people who can read Python and SQL. These readers want to understand Spark well enough to think clearly about it, not only to use it.

[Start with Chapter 1: From MapReduce to Spark](chapters/ch01-mapreduce-to-spark.md){ .md-button .md-button--primary }
[Go to the hands-on start: Chapter 5](chapters/ch05-how-spark-runs.md){ .md-button }
[See the whole roadmap](roadmap.md){ .md-button }

## What every chapter has

- **The exact code that ran**, as a file in [`code/`](https://github.com/open-lakehouse/spark-masterclass/tree/main/code), pinned to `pyspark==4.3.0`.
- **Its real output**, saved next to it. The output includes the plans that Spark printed and the numbers that the Spark UI reported.
- **Diagrams of the mechanism**: where the data goes, which operator runs, and what the engine keeps.
- **Figures and animations** made from the saved output, so that you can see a shuffle, a plan or a commit move.
- **Videos of the code as it runs**, where a video shows more than the text.

## The parts

| Part | Chapters | What it covers |
|---|---|---|
| I. [Background](chapters/ch01-mapreduce-to-spark.md) | 1 to 4 | How Spark came to be, how it runs, and how it became a batch and streaming platform |
| II. [Getting started](chapters/ch05-how-spark-runs.md) | 5 to 8 | Your first application, the install, a quick start and configurations |
| III. [DataFrames and the other APIs](chapters/ch09-dataframes-columns-schemas.md) | 9 to 14 | Columns and schemas, cleaning values, joins, RDDs, the pandas API on Spark, typed Datasets |
| IV. [Spark SQL](chapters/ch15-spark-sql.md) | 15 to 23 | The language and its tools, tables and DDL, Hive, windows and pipe syntax, aggregation, procedural SQL, time series, geospatial, metric views |
| V. [Spark Connect](chapters/ch24-spark-connect.md) | 24 to 30 | Why Connect, the protocol, observability, clients in other languages, building and writing clients, production |
| VI. [Files, formats and data sources](chapters/ch31-files-as-files.md) | 31 to 39 | Files, logs, CSV, JSON, complex types, VARIANT, XML, Avro, Protobuf, blobs, object storage, JDBC |
| VII. [Performance](chapters/ch40-query-plans.md) | 40 to 47 | Plans, statistics and the optimizer, partitions and memory, Python and Arrow, Parquet and ORC, Project Feather, acceleration |
| VIII. [Streaming](chapters/ch48-structured-streaming.md) | 48 to 53 | Structured Streaming, messy streams, transformWithState, Real-time Mode, checkpoints, Kafka |
| IX. [Lakehouse tables](chapters/ch54-tables-delta-iceberg.md) | 54 to 55 | Delta and Iceberg, the Iceberg runtime and catalogs |
| X. [Declarative Pipelines](chapters/ch56-first-etl-pipeline.md) | 56 to 62 | Your first ETL pipeline, the CLI and project, configuration, the model, medallion, CDC, how SDP runs |
| XI. [Machine learning and applied algorithms](chapters/ch63-mllib-mlflow.md) | 63 to 69 | MLlib and MLflow, clustering, recommendation and patterns, text, similarity search, entity resolution, sketches, calling models |
| XII. [Running Spark in production](chapters/ch70-submitting-applications.md) | 70 to 78 | Submitting applications, security, determinism, dependencies, observability, testing, Kubernetes, orchestration, agents |
| XIII. [Extending Spark](chapters/ch79-python-data-sources-readers.md) | 79 to 87 | Python data sources, DSv2 in Java, Catalyst extensions, Connect plugins, a capstone |
| XIV. [Internals](chapters/ch88-catalyst-plans-python.md) | 88 to 92 | Catalyst and Python, the worker protocol, VARIANT bytes, an upstream case study, building Spark |

The [coverage](coverage.md) page shows where each SparkTutor lesson is in the book.

## Run the code

```bash
git clone https://github.com/open-lakehouse/spark-masterclass && cd spark-masterclass
code/common/get_data.sh                     # one month of NYC taxi trips, about 65 MB
cd code && uv sync                          # Python 3.11+, pyspark 4.3.0; needs Java 17, 21 or 25
uv run python ch07/quickstart.py
```

Some chapters need more than this: Docker (catalogs, Kafka, Kubernetes with kind), extra Maven packages, or an NVIDIA GPU. Each chapter says what it needs at the start.

The license of the book is Apache 2.0, the same as Spark. You can send corrections as issues or pull requests.
