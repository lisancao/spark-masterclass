# Writing a Spark Masterclass chapter

This is the standard for every chapter after Part I. Part I (`book/chapters/ch01-*.md` to `ch04-*.md`, with `code/ch01` to `code/ch04`) is the worked example: read all four chapters and their code before writing anything.

## What a chapter is

A chapter teaches one subject in depth, at the depth of a long technical blog post, and covers everything the SparkTutor lessons mapped to it teach (see `book/coverage.md`). It is not a summary of those lessons. The lessons are the floor for coverage; the chapter goes further by showing what Spark actually does.

Every chapter has:

- **A thesis.** A loose opener, then the chapter's claim in bold. The chapter argues it step by step.
- **Steps.** `## Step N: ...` headings. Each step makes one point and shows the artifact that proves it: code, its real output, a plan, a UI or REST record, a file listing, a hexdump.
- **Mermaid diagrams** (two to four) wherever a structure or a sequence is easier to see than to read: plans, stage graphs, state machines, data layouts, request flows. Each diagram must match what the code showed.
- **Code that ran.** Every snippet in the text comes from a file under `code/chNN/` that ran on `pyspark==4.3.0`, and every output block is pasted from `code/chNN/output/*.txt`. No invented output, no "output will look like".
- **Film clips** of the code running in a terminal (two to four per chapter, at least one) for the moments that are better watched: a job starting, a stream ticking, a failure and its recovery, a plan changing. Use the `film-demo` skill's VHS kit, `--format chapter` (1280x720, about 95x24 cells, so keep output lines under about 90 columns), cut clips into `book/media/clips/` as `.mp4` plus a `.webp` poster, and embed them the way Part I does (`<figure markdown><video autoplay loop muted playsinline controls ...>`).
- **"The whole chapter, on one page"** as the closing section: the chapter's code in one runnable block, or a compact recap of the steps.
- **New in 4.3** called out where it applies, checked against the 4.3 release notes, docs or source, not memory.

## Voice and rules

- Load the `zuma-voice` skill before writing prose. Write for smart readers: dense, specific, first person where it fits, no filler, no listicles.
- No em dashes (U+2014) anywhere. Use semicolons, parentheses, commas or periods.
- No decorative numbers. Numbers in the text are measurements, versions, configs or results, and each one traces to saved output.
- If a claim cannot be verified by running it or by reading Spark's source or docs, cut it or say plainly that it is unverified.
- Something that looks like a Spark bug goes into your part notes as an open finding, not into the book as a bug.

## Running code

- `code/` is a uv project: `cd code && uv sync`, then `uv run python chNN/script.py`. Shared helpers are in `code/common/book.py` (`session`, `plan`, `ui_api`, data paths). Add helpers there only if several chapters need them, and say so in your notes.
- Set `BOOK_DATA=$HOME/spark-masterclass/data` so you read the one shared download (the taxi month is already there; `code/common/get_data.sh` fetches more months into it if a chapter needs more data).
- Use your assigned `SPARK_UI_PORT` and Connect port so you don't collide with the other writers running at the same time.
- Wrap every timing measurement and every film render in `flock $HOME/spark-masterclass/data/.bench.lock <command>` so timings are taken on a quiet machine. Report timings as medians over repeated runs after a warmup, and record the run conditions.
- Saved outputs must not contain the home directory: pipe through `sed "s#$HOME#~#g"` when saving.
- Data: the NYC taxi trips where they fit. For messy-data topics (logs, CSV drift, JSON, XML, documents, blobs) reuse SparkTutor's sample data or generators from `~/Documents/spark_content/sparktutor` (copy small files into `code/data/<topic>/` or add a generator script); keep anything large out of git.
- External packages (Delta, Iceberg, Kafka, Avro, the Kubernetes operator): find the release that supports Spark 4.3 / Scala 2.13 and record the exact coordinates. If none exists yet, say so in your notes and in the chapter, and use the closest supported setup, clearly labelled.
- Never use `pkill -f` (it can kill your own shell); stop processes by PID.

## Files you own

Work only in your git worktree, on your branch. Write only:

- `book/chapters/chNN-<slug>.md` for your chapters
- `code/chNN/` (scripts and `output/`)
- `code/film/chNN.tape`, and clips `book/media/clips/chNN-*`
- `book/media/clips/clips-partNN.tsv` (your clip cut list, same columns as `clips.tsv`)
- `notes/partNN.md`

Do not edit `mkdocs.yml`, `book/roadmap.md`, `book/coverage.md`, `NOTES.md`, `README.md` or other parts' files; the integrator does that. Check your chapters build with `$HOME/spark-masterclass/.docs-venv/bin/mkdocs build --strict -d <a scratch dir>/site`; pages outside the nav are fine.

## Your part notes (`notes/partNN.md`)

1. **Coverage table**: every SparkTutor lesson mapped to your part, each of its topics, and the chapter section that covers it, or "left out" with the reason.
2. **Claim-to-source table**: each number and non-obvious claim in your chapters, and the output file or source/doc link it comes from.
3. **Environment**: versions and package coordinates you used.
4. **Open findings**: possible Spark bugs or doc gaps, with a minimal repro.
5. **Nav entries**: the `mkdocs.yml` nav lines for your chapters.

Commit to your branch as you finish each chapter (plain commit messages, no attribution lines). Do not push.
