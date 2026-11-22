# Spark Masterclass

**An open, in-depth book about Apache Spark 4.3. Each example runs on your laptop, and each number comes from a run that you can do again.**

Most Spark material stops at the API. Other material assumes that a managed platform does the important parts, behind a paywall. This book does the opposite. It starts from a `pip install`, and it uses one month of real New York taxi trips. At each step, it asks what Spark did. It examines the plans that Spark built, the jobs and tasks that Spark ran, the files that Spark wrote and the bytes that Spark moved.

The book is for people who can read Python and SQL. These readers want to understand Spark well enough to think clearly about it, not only to use it.

[Start with Chapter 1: How Spark runs your code](chapters/ch01-how-spark-runs.md){ .md-button .md-button--primary }
[See the whole roadmap](roadmap.md){ .md-button }

## What every chapter has

- **The exact code that ran**, as a file in [`code/`](https://github.com/lisancao/spark-masterclass/tree/main/code), pinned to `pyspark==4.3.0`.
- **Its real output**, saved next to it. The output includes the plans that Spark printed and the numbers that the Spark UI reported.
- **Diagrams of the mechanism**: where the data goes, which operator runs, and what the engine keeps.
- **Videos of the code as it runs**, where a video shows more than the text.

## The parts

| Part | Chapters | What it covers |
|---|---|---|
| I. [Getting started](chapters/ch01-how-spark-runs.md) | 1 to 4 | How Spark runs a query, four methods to install Spark 4.3, a quick start, Spark Connect |
| II. [DataFrames and SQL](chapters/ch05-dataframes-columns-schemas.md) | 5 to 10 | Schemas, cleaning values, Spark SQL in depth, procedural SQL, joins, time series and geospatial |
| III. [Files, formats and messy data](chapters/ch11-files-as-files.md) | 11 to 17 | Files, logs and CSV, JSON, complex types, VARIANT, XML, Avro and Protobuf, documents and blobs |
| IV. [Performance](chapters/ch18-query-plans.md) | 18 to 24 | Query plans, partitions and memory, Python UDFs and Arrow, UDTFs, Parquet, Project Feather, native and GPU engines |
| V. [Streaming](chapters/ch25-structured-streaming.md) | 25 to 29 | Structured Streaming, messy streams, `transformWithState`, Real-time Mode, checkpoints and the state store |
| VI. [Pipelines and tables](chapters/ch30-tables-delta-iceberg.md) | 30 to 36 | Delta and Iceberg, Iceberg catalogs, Declarative Pipelines, medallion, change data capture, metric views |
| VII. [Machine learning and applied algorithms](chapters/ch37-mllib-mlflow.md) | 37 to 42 | MLlib and MLflow, text search, similarity search, entity resolution, sketches, calls to models |
| VIII. [Running Spark in production](chapters/ch43-determinism-retries.md) | 43 to 49 | Retries, Python dependencies, observability, Connect in production, Kubernetes, orchestration, agents |
| IX. [Extending Spark](chapters/ch50-python-data-sources-readers.md) | 50 to 58 | Python data sources, a DSv2 connector in Java, Catalyst extensions, Connect plugins, a capstone connector |
| X. [Internals](chapters/ch59-catalyst-plans-python.md) | 59 to 62 | Catalyst and Python, the Python worker protocol, the VARIANT format, an upstream bug from report to fix |

The [coverage](coverage.md) page shows where each SparkTutor lesson is in the book.

## Run the code

```bash
git clone https://github.com/lisancao/spark-masterclass && cd spark-masterclass
code/common/get_data.sh                     # one month of NYC taxi trips, about 65 MB
cd code && uv sync                          # Python 3.11+, pyspark 4.3.0; needs Java 17, 21 or 25
uv run python ch03/quickstart.py
```

Some chapters need more than this: Docker (catalogs, Kafka, Kubernetes with kind), extra Maven packages, or an NVIDIA GPU. Each chapter says what it needs at the start.

The license of the book is Apache 2.0, the same as Spark. You can send corrections as issues or pull requests.
