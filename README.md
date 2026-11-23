# ⚡ Spark Masterclass

Most Spark material teaches the API. Spark Masterclass teaches the machine the API wakes up.

This is an open, example-driven book about Apache Spark 4.3: how a DataFrame becomes a plan, where Python actually runs, what stateful streaming remembers, why a connector receives one filter and not another, and how the same engine behaves from a laptop to production. Its 92 chapters span execution fundamentals, SQL, Spark Connect, data formats, performance, streaming, lakehouse tables, Declarative Pipelines, machine learning, operations, extension APIs, and internals.

Every technical result is tied to runnable code and saved output in this repository. Figures and short animations make invisible mechanisms visible. Every example is designed to run on a laptop.

**[Read the book online](https://open-lakehouse.github.io/spark-masterclass/)** · **[Browse all 92 chapters](mkdocs.yml)** · **[Run the first example](#run-an-example)**

## ✨ What makes this different

| Ordinary Spark material | Spark Masterclass |
|---|---|
| Shows an API call and its output | Follows the call through planning, execution, storage, and failure |
| Explains one subsystem in isolation | Connects SQL, streaming, Python, catalogs, pipelines, and operations into one engine |
| Uses prose examples that drift | Stores the runnable script and the output behind each technical claim |
| Stops at application code | Builds clients, connectors, data sources, Catalyst extensions, and production workflows |

The result is both a learning path and an executable reference: read it front to back, or enter at the mechanism that is currently breaking, surprising, or slowing down a Spark system.

## 🧭 Choose a path

| Goal | Start here |
|---|---|
| Understand how Spark runs code | Part I, then [Chapter 5](book/chapters/ch05-how-spark-runs.md) |
| Become productive with DataFrames and SQL | Parts III and IV |
| Build on Spark Connect | Part V, from architecture through client and application design |
| Diagnose performance | Part VII: plans, statistics, partitions, Python, Parquet, and acceleration |
| Build stateful streaming systems | Part VIII: Structured Streaming, `transformWithState`, Real-Time Mode, checkpoints, and Kafka |
| Build lakehouse pipelines | Parts IX and X: Iceberg and Spark Declarative Pipelines |
| Operate Spark in production | Part XII: submission, security, retries, dependencies, observability, testing, Kubernetes, and orchestration |
| Extend the engine | Parts XIII and XIV: data sources, connectors, Catalyst, Connect plugins, and internals |

The complete chapter list is in the [site navigation](mkdocs.yml). The [coverage page](book/coverage.md) records the scope of each part.

## 📋 Requirements

- Python 3.11 or later
- Java 17, 21, or 25
- [`uv`](https://docs.astral.sh/uv/) for the chapter environment
- A POSIX shell for the data and utility scripts

Some chapters use additional local services or build tools. Those requirements are introduced in the relevant chapter.

<a id="run-an-example"></a>

## 🚀 Run an example

Download the pinned NYC taxi data and create the Python environment:

```bash
./code/common/get_data.sh
cd code
uv sync
```

Run a chapter script from `code/`:

```bash
uv run python ch05/anatomy.py
```

Each chapter directory contains the source used by the book. Saved results live beside the examples in an `output/` directory when the chapter records output.

## 📚 Build the book

Create a documentation environment and start the local site:

```bash
python -m venv .docs-venv
.docs-venv/bin/pip install -r requirements-docs.txt
.docs-venv/bin/mkdocs serve
```

Open <http://127.0.0.1:8000>. GitHub Actions publishes `main` to the [public site](https://open-lakehouse.github.io/spark-masterclass/).

## 🗺️ Repository layout

| Path | Purpose |
|---|---|
| `book/` | Book chapters, figures, animation posters, and site pages |
| `code/` | Runnable examples and recorded output, organized by chapter |
| `code/common/` | Shared helpers, data download, and figure tooling |
| `mkdocs.yml` | Site configuration and complete table of contents |
| `scripts/` | Documentation checks and maintenance utilities |
| `notes/` | Source and verification notes used to maintain the book |

## 🧪 Documentation checks

The prose uses ASD-STE100 Simplified Technical English. Run the repository checker against a Markdown file:

```bash
./scripts/ste-check book/chapters/ch05-how-spark-runs.md
```

## 📜 License

The project is licensed under the [Apache License 2.0](LICENSE). The examples download, but do not redistribute, trip data published by the New York City Taxi and Limousine Commission.
