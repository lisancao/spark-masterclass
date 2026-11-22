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

## Part I: Getting started

1. [How Spark runs your code](chapters/ch01-how-spark-runs.md): the path of one query from a DataFrame to jobs, stages, tasks and Python workers. The chapter also shows what Project Feather saves on small queries.
2. [Installing Spark 4.3](chapters/ch02-installing.md): pip, the tarball, Docker and Spark Connect, each one tested with a real run, and how to choose.
3. [Quick start](chapters/ch03-quick-start.md): one question about taxi trips in DataFrames and in SQL. The chapter also shows how to clean the data for it, and the plan that the two APIs compile to.
4. [Spark Connect](chapters/ch04-spark-connect.md): the unresolved plan that a client sends, and where errors occur. The chapter also shows how to follow one operation from the client to the server with the operation IDs of 4.3.

The other parts are on the [roadmap](roadmap.md): DataFrames and SQL, messy data, performance, streaming, pipelines and tables, machine learning, production, extending Spark, and internals.

## Run the code

```bash
git clone https://github.com/lisancao/spark-masterclass && cd spark-masterclass
code/common/get_data.sh                     # one month of NYC taxi trips, about 65 MB
cd code && uv sync                          # Python 3.11+, pyspark 4.3.0; needs Java 17, 21 or 25
uv run python ch03/quickstart.py
```

The license of the book is Apache 2.0, the same as Spark. You can send corrections as issues or pull requests.
