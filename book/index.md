# Spark Masterclass

**An open, in-depth book on Apache Spark 4.3, where every example runs on your laptop and every number comes from a run you can repeat.**

Most Spark material stops at the API, or assumes a managed platform is doing the interesting parts behind a paywall. This book goes the other way: it starts from a `pip install`, works through one month of real New York taxi trips, and keeps asking what Spark actually did, reading the plans it built, the jobs and tasks it ran, the files it wrote and the bytes it moved. It's written for people who can read Python and SQL and want to understand Spark well enough to reason about it, not just to use it.

[Start with Chapter 1: How Spark runs your code](chapters/ch01-how-spark-runs.md){ .md-button .md-button--primary }
[See the whole roadmap](roadmap.md){ .md-button }

## What every chapter has

- **The exact code that ran**, as a file in [`code/`](https://github.com/lisancao/spark-masterclass/tree/main/code), pinned to `pyspark==4.3.0`.
- **Its real output**, saved next to it, including the plans Spark printed and the numbers the Spark UI reported.
- **Diagrams of the mechanism**: where data goes, which operator runs, what the engine keeps.
- **Recordings of it running**, where watching it says more than a listing.

## Part I: Getting started

1. [How Spark runs your code](chapters/ch01-how-spark-runs.md): one query followed from a DataFrame to jobs, stages, tasks and Python workers, plus what Project Feather saves on small queries
2. [Installing Spark 4.3](chapters/ch02-installing.md): pip, the tarball, Docker and Spark Connect, each run for real, and how to pick
3. [Quick start](chapters/ch03-quick-start.md): one question about taxi trips in DataFrames and in SQL, the cleaning it needs, and the plan both compile to
4. [Spark Connect](chapters/ch04-spark-connect.md): the unresolved plan a client sends, where errors appear, and following one operation from client to server with 4.3's operation IDs

The remaining parts (DataFrames and SQL, messy data, performance, streaming, pipelines and tables, machine learning, production, extending Spark, and internals) are on the [roadmap](roadmap.md).

## Running the code

```bash
git clone https://github.com/lisancao/spark-masterclass && cd spark-masterclass
code/common/get_data.sh                     # one month of NYC taxi trips, about 65 MB
cd code && uv sync                          # Python 3.11+, pyspark 4.3.0; needs Java 17, 21 or 25
uv run python ch03/quickstart.py
```

The book is licensed under Apache 2.0, like Spark. Corrections are welcome as issues or pull requests.
