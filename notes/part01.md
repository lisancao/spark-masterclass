# Part I notes: STE rewrite

Part I prose (`book/index.md`, `ch05` to `ch24`, and the prose of `book/roadmap.md` and `book/coverage.md`) is now in ASD-STE100 Simplified Technical English, following the "Language" section of `notes/chapter-brief.md`. Code blocks, output blocks, plans, commands, mermaid diagrams, video embeds, links and numbers did not change (checked by comparing every fence, `<video>`/`<source>` line, link target and number token against the previous commit). Tables in `roadmap.md` and `coverage.md` did not change.

## Technical names (TN)

- Products and projects: Spark, Apache Spark, PySpark, Spark Connect, Project Feather, Parquet, CSV, Arrow, Py4J, gRPC, protobuf, JVM, JDK, OpenJDK, Docker, Kubernetes, YARN, uv, pip, pandas, pyarrow, grpcio, Hadoop, Scala, Java, Python, SQL, OCaml, Go, Rust, Swift, NYC Taxi and Limousine Commission (TLC), SparkTutor.
- Spark concepts: driver, executor, cluster manager, task, task slot, stage, job, query stage, shuffle, exchange, partition, row group, footer, plan (logical, analyzed, optimized, physical), analyzer, optimizer, planner, scheduler, transformation, action, adaptive query execution (AQE), partition coalescing, column pruning, hash partitioning, range partitioning, single-task execution, whole-stage code generation, broadcast hash join, temporary view, session, operation ID, job tag, event log, history server, monitoring REST API, Spark UI, local mode, Classic, thin client, Connect server, streaming (as the API name), unresolved plan.
- Code identifiers in code format: config keys, class and operator names (`HashAggregate`, `Exchange`, `ArrowEvalPython`, `TakeOrderedAndProject`, `AQEShuffleRead`), methods, file paths, commands.
- Titles kept as names: chapter titles (for example "Installing Spark 4.3", which is also the `mkdocs.yml` nav entry), part titles ("Extending Spark", "Machine learning"), the course name "Learning Spark", linked post titles ("Building a Spark Connect Client in OCaml", "An Idiot's Guide to Spark Connect Clients").

## Technical verbs (TV)

compile, deploy, install, download, parse, analyze, optimize, schedule, shuffle, partition, materialize, coalesce, prune, push (a filter), serialize, normalize, fuse, fork, stream, decode, infer, query, register, resolve, debug, migrate, upgrade, crash, log, commit, compute, filter, merge, sort, sample.

## stecheck finding counts

`stecheck --mode description <file>` on the raw file. "Before" is the previous commit.

| File | Before | After (raw) | After (prepared, allowlist) |
|---|---|---|---|
| `book/index.md` | 22 | 6 | 0 |
| `book/chapters/ch05-how-spark-runs.md` | 120 | 28 | 0 |
| `book/chapters/ch06-installing.md` | 53 | 10 | 0 |
| `book/chapters/ch07-quick-start.md` | 72 | 7 | 0 |
| `book/chapters/ch24-spark-connect.md` | 80 | 23 | 0 |
| `book/roadmap.md` | 5 | 0 | 0 |
| `book/coverage.md` | 2 | 2 | 0 |

Every raw finding left after the rewrite is a false positive of the lint:

- **List items joined into one sentence.** stecheck reads a tight markdown list as one paragraph and joins the last sentence of one item to the next item. Each item, checked alone, is within the limit.
- **`<figure>` HTML.** The `<video>` and `<source>` attributes count as words in the caption sentence.
- **Bold ends.** A sentence that ends in `.**` is joined to the next sentence (the thesis paragraphs).
- **Words in link targets.** URLs such as `data-engineering-ai`, `sql-programming-guide` and `ch06-installing.md` give `-ing` findings.

"Prepared" means the file went through a small filter before stecheck: link targets removed (link text kept), HTML tags removed (caption text kept), `**` removed, and a blank line put before each list item. The allowlist is `partitioning pruning coalescing monitoring tuning warning streaming` for all files, plus `installing learning extending` (titles) in `index.md`, `learning` (course name) in `coverage.md`, and `unresolved building` (TN adjective, post title) in `ch24`.

Procedure text (install and run steps in ch06, ch24 and the "How to choose" list) was also read against `--mode procedure`. All procedure sentences are imperative, 20 words or fewer, one instruction each, with the condition first. The `--mode procedure` length findings in ch06 are all description sentences.

## Deliberate rule breaks

- **Passive or past-participle forms kept:** "released at the end of October 2026" and "pinned by tag" (participles used as adjectives). "Unresolved" is a TN adjective ("unresolved plan").
- **`-ing` words kept as technical names:** hash partitioning, range partitioning, partition coalescing, column pruning, monitoring REST API, streaming (API name), warning (the admonition noun).
- **Parentheses** are kept where they hold a config key, a JIRA number, a code example list or a short label, as in the original. A few hold a short explanation ("(a few kilobytes)", "(16 on the machine that gave these numbers)"). These are labels for the preceding noun, not second thoughts.
- **Chapter and part titles** are not rewritten (they are names, and the nav in `mkdocs.yml` belongs to the integrator). Step headings were rewritten where they had a problem, and one anchor changed: `#getting-the-data` is now `#get-the-data` (the only link to it is in ch06, updated).
- **Figure captions** are prose and were rewritten. The video markup is unchanged.

## Content changes beyond sentence form

- First person removed. "When I first learned Spark I treated it like a very large pandas" became "Many new users think of Spark as a very large pandas". "I ran every install" became "Each install in this chapter ran". "my laptop" became "the laptop that ran the script". "I checked" became "A test confirmed this behavior". "I use the pip install here" became "This chapter uses the pip install". "My rule of thumb" and "I wrote a longer tour" / "I wrote one in OCaml" became "The author ...".
- The OCaml joke ("about as sensible as it sounds") became "which is not a sensible choice for most people", since STE has no ironic register.
- ch05 Step 4: "my suspicion is that most of that is start-up cost" became "A possible cause is start-up cost ... This cause is not verified." Same hedge, stated plainly.
- ch06: "a checksum served next to a tampered file proves nothing" became "If a mirror serves a changed file, a checksum from the same mirror proves nothing".
- ch06: the thin-client admonition now starts with the command "Do not paste thin-client code into `python -c`", and its title is "Do not use `python -c` with the thin client". The facts (status 255, the message, the doctest check, the cases that work) are all kept.
- ch06: "uv refused to resolve the project until I raised `requires-python` from 3.10" became "uv did not resolve the project until the book increased `requires-python` from 3.10".
- ch24: "Before 4.3" added twice to replace "used to", so the version number 4.3 appears two more times. No other number was added or removed.
- Several long sentences became vertical lists (ch05 Part IV rules, ch06 tarball contents and data problems, ch24 Classic problems, `sparkContext` replacements, operation record, Connect use cases, coverage abbreviations). The items are the same as in the original sentences.

## Words not confirmed as approved

The official dictionary is not available here, so these words were used without confirmation. Some are probably unapproved and need a TN/TV decision or a replacement:

lazily, explicitly, automatically, immediately, locally, directly, clearly, adjacent (removed), sensible, reasonable, reliable, reproducible, dense, residential, flat (fare), surprise, wraps, communicates, arrangement, opinions, noise, practice(s), habit (removed), purpose, suggestion, quantities, overhead, workloads, boundary, midpoint, horizontal, interactive, intermediate, commutative, operands, numerator, denominator, ratio, weight, allocation, estimate, rite of passage (removed), "deep in the server", "front door" (ch24 recap, a metaphor kept from the original).

"Thus" is used as the connecting word for results. "Possibly", "probably" and "most likely" carry the original hedges.
