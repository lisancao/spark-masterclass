# Writing a Spark Masterclass chapter

This is the standard for every chapter after Part I. Part I (`book/chapters/ch01-*.md` to `ch04-*.md`, with `code/ch01` to `code/ch04`) is the worked example: read all four chapters and their code before writing anything.

## What a chapter is

A chapter teaches one subject in depth, at the depth of a long technical blog post, and covers everything the SparkTutor lessons mapped to it teach (see `book/coverage.md`). It is not a summary of those lessons. The lessons are the floor for coverage; the chapter goes further by showing what Spark actually does.

Every chapter has:

- **A thesis.** A short opening paragraph, then the chapter's claim in bold. The chapter supports it step by step.
- **Steps.** `## Step N: ...` headings. Each step makes one point and shows the artifact that proves it: code, its real output, a plan, a UI or REST record, a file listing, a hexdump.
- **Mermaid diagrams** (two to four) wherever a structure or a sequence is easier to see than to read: plans, stage graphs, state machines, data layouts, request flows. Each diagram must match what the code showed.
- **Code that ran.** Every snippet in the text comes from a file under `code/chNN/` that ran on `pyspark==4.3.0`, and every output block is pasted from `code/chNN/output/*.txt`. No invented output, no "output will look like".
- **Film clips** of the code running in a terminal (two to four per chapter, at least one) for the moments that are better watched: a job starting, a stream ticking, a failure and its recovery, a plan changing. Use the `film-demo` skill's VHS kit, `--format chapter` (1280x720, about 95x24 cells, so keep output lines under about 90 columns), cut clips into `book/media/clips/` as `.mp4` plus a `.webp` poster, and embed them the way Part I does (`<figure markdown><video autoplay loop muted playsinline controls ...>`).
- **"The whole chapter, on one page"** as the closing section: the chapter's code in one runnable block, or a compact recap of the steps.
- **New in 4.3** called out where it applies, checked against the 4.3 release notes, docs or source, not memory.

## Language: ASD-STE100 Simplified Technical English

The book's prose is written in ASD-STE100 Simplified Technical English (STE). Load the `asd-ste100` skill before you write, and keep its `rules.md` and `words.md` open. STE replaces a personal blog voice: the book must read clearly for non-native readers and for translation tools.

- **Classify each block.** Explanations of how Spark works are *description* text (25 words or fewer per sentence). Things the reader does (install, run, change a config) are *procedure* text (20 words or fewer, imperative, one instruction per sentence, conditions before commands). Warnings and cautions use the STE safety-instruction form, in admonitions.
- **Technical names and technical verbs** (API names, config keys, commands, operator names in plans, file formats, `shuffle`, `partition`, `broadcast`, `cache`, `spill`, `commit`) are allowed. Put code identifiers in code format. Use each term with one meaning in the whole chapter. List the technical names and verbs at the top of your part notes.
- **Rules that catch most problems:** one topic per sentence; active voice; no `-ing` verb forms; no phrasal verbs; noun clusters of three words or fewer; keep articles and "that"; no semicolons (STE forbids them) and no em dashes (Lisa's rule). Split the sentence or use a vertical list.
- **Keep the structure.** The chapter still has a thesis (a short bold statement after the opening paragraph, in STE), `## Step N:` headings, diagrams, code, output and clips. STE changes the sentences, not the depth. Do not remove content to make a sentence short; split it.
- **Code, commands, output, plans and literal UI text stay exactly as they are.** STE applies to the prose around them. Mermaid labels are short phrases and follow STE word choice where they are words.
- **Run the checker on every chapter:** `scripts/ste-check book/chapters/chNN-<slug>.md` (it wraps `stecheck --mode description`, skips code blocks and mermaid, and removes markdown noise such as link targets and video tags; shared allowlist in `scripts/ste-allow.txt`, add chapter-specific words with `--allow`). Fix every finding, or allowlist a technical name with `--allow`. Record the final finding count per chapter, and each rule you broke on purpose with the reason, in your part notes. Do not call the text "certified" or "fully compliant"; the checker cannot confirm the official dictionary.
- No decorative numbers. Numbers in the text are measurements, versions, configs or results, and each one traces to saved output.
- If a claim cannot be verified by running it or by reading Spark's source or docs, cut it or say plainly that it is not verified.
- Something that looks like a Spark bug goes into your part notes as an open finding, not into the book as a bug.

## Running code

- `code/` is a uv project: `cd code && uv sync`, then `uv run python chNN/script.py`. Shared helpers are in `code/common/book.py` (`session`, `plan`, `ui_api`, data paths). Add helpers there only if several chapters need them, and say so in your notes.
- Set `BOOK_DATA=$HOME/spark-masterclass/data` so you read the one shared download (the taxi month is already there; `code/common/get_data.sh` fetches more months into it if a chapter needs more data).
- Use your assigned `SPARK_UI_PORT` and Connect port so you don't collide with the other writers running at the same time.
- Use `flock $HOME/spark-masterclass/data/.bench.lock <command>` **only for runs whose timings the book quotes** (benchmarks, latency measurements). Do not take the lock for films, for scripts whose output has no quoted timing, or for setup. Keep each locked run short (minutes, not tens of minutes): measure on a smaller input or fewer repetitions if needed. Films run without the lock; a film may show timings on screen, but the text quotes only timings from locked runs saved in `output/`. Report timings as medians over repeated runs after a warmup, and record the run conditions.
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
