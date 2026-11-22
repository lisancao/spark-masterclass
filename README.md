# Spark Masterclass

An open, in-depth book on Apache Spark 4.3, in fourteen parts and 77 chapters, with figures and animations for each mechanism, written in ASD-STE100 Simplified Technical English. Every example runs on a laptop, and every number in the text comes from a script in this repository and its saved output.

**Read it online:** https://lisancao.github.io/spark-masterclass/

## Layout

| Path | What's there |
|---|---|
| `book/` | The book's Markdown, figures and clip posters (an MkDocs site) |
| `code/` | One folder per chapter: the scripts the chapter runs and their saved `output/` |
| `code/common/` | Shared helpers and `get_data.sh`, which downloads the pinned NYC taxi data |
| `notes/` | One notes file per writer: SparkTutor coverage, where each number came from, versions, and open findings |
| `NOTES.md` | The same for the first Getting started chapters, and the environment |
| `scripts/` | `ste-check` (the Simplified Technical English lint) and the renumber scripts. The figure kits are in `code/common/` |

## Run the code

```bash
code/common/get_data.sh            # June 2026 NYC yellow-taxi trips + zone lookup into data/
cd code && uv sync                 # Python 3.11+, pyspark[connect]==4.3.0; needs Java 17, 21 or 25
uv run python ch05/anatomy.py
```

## Build the book

```bash
pip install -r requirements-docs.txt
mkdocs serve                       # http://127.0.0.1:8000
```

The site deploys to GitHub Pages from `main` through `.github/workflows/deploy-docs.yml`.

## License

Apache License 2.0. The NYC trip data is published by the NYC Taxi and Limousine Commission and is downloaded, not redistributed.
