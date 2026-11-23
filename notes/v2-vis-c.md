# v2-vis-c: visual aids for chapters 33 to 45

Scope: figures, animations, tables and captions added to existing chapters 33 to 45. The prose substance is not changed. In ch50 to ch52 the edits are only added figures, tables and captions (another writer changes links there).

## How the media are made

- **Figures** are SVG, drawn by `code/chNN/figures/make.py` with the shared kit `code/common/figkit.py` (new helper, used by every chapter of this slice). Each figure has a `-light.svg` and a `-dark.svg` file that differ only in the CSS colour variables, embedded with `#only-light` and `#only-dark`. Run `python3 chNN/figures/make.py <png-dir>` from `code/` to write the SVGs and PNG checks (headless Chrome on white and on the slate background).
- **Animations** are HyperFrames scenes (create-animation skill, theme `paper`, landscape) in `code/chNN/anim/<name>/` with `storyboard.md` and `data.json`. `anim render code/chNN/anim/<name>` renders 1920x1080. The book copy is scaled to 1280x720, H.264 CRF 30, no audio, with a WebP poster: `book/media/clips/chNN-anim-<name>.{mp4,webp}`.
- In each `<figure markdown>`, the two images and the caption are separate paragraphs (blank lines), so `scripts/ste-check` checks the caption as its own sentences.

## Per chapter

| Chapter | Figures | Animation | Tables and other elements | Sources of the numbers |
|---|---|---|---|---|
| 33 | `ch50-routing` (run 1 rows to silver, quarantine or not kept), `ch50-rules` (quarantine counts per rule), `ch50-gate-timeline` (run 2 truncate, failed attempts, skipped gold) | `ch50-anim-routing` | layer contract table (Step 1), file source options table (Step 2) | `code/data/retail/orders/`, `code/ch50/output/two_runs.txt`, `code/ch50/output/gate.txt` |
| 34 | `ch51-three-ways` (append, arrival order, by seq for c-01), `ch51-history` (versions on a seq axis, all columns against TRACK HISTORY ON (city)), `ch51-aux` (targets and auxiliary state tables) | `ch51-anim-scd2` (SCD2 closes and opens versions; the late seq 4 event) | `deduplicationMode`/`computeUpdates` options table (Step 2) | change files in Step 1, `code/ch51/output/drops.txt`, `internals.txt` |
| 35 | `ch17-ratios` (fare per mile by borough, average of ratios against ratio of totals), `ch17-stored` (YAML to column metadata and table properties) | `ch17-anim-measure` (MEASURE() rewrite, two grains, the second aggregation) | YAML key table (Step 2), lifecycle command table (Step 8) | `code/ch17/output/two_errors.txt`, `define.txt`, `grains.txt` |
| 36 | `ch52-five-steps` (StartRun steps, the dry-run line, effects), `ch52-sink-log` (checkpoint versions against the file sink log) | `ch52-anim-flows` (flows of run 2 complete in topological order, at the COMPLETED times) | PipelineCommand table (Step 3) | `PipelineExecution.scala` v4.3.0, Step 8 table, `code/ch52/output/end_to_end.txt` |
| 37 | `ch53-vector` (rate code through StringIndexer, OneHotEncoder, VectorAssembler into 7 named slots), `ch53-mae` (MAE by rate code, both models) | `ch53-anim-treeaggregate` (one treeAggregate pass, then the loop to 11 jobs; partitions and speed schematic) | `handleInvalid` table (Step 3) | `code/ch53/output/estimators.txt`, `fare_pipeline.txt` |
| 38 | `ch54-tokens` (one sentence through the tokenizers and stop words), `ch54-collisions` (words that share a HashingTF slot per numFeatures) | `ch54-anim-lsh` (messages into buckets, candidates, exact distances; buckets schematic) | CountVectorizer against HashingTF table (Step 4) | `code/ch54/output/` (tokenizer, HashingTF and near-duplicate output) |
| 39 | `ch55-tradeoff` (recall against work, BRP LSH and IVF), `ch55-scurve` (banding S-curves from the formula, with measured candidate shares) | `ch55-anim-ivf` (nearest lists light up for nprobe 1 to 16; positions schematic) | | `code/ch55/output/indexes.txt` |
| 40 | `ch56-sweep` (precision and recall per threshold), `ch56-wrong-edge` (one wrong match makes 9 wrong pairs), `ch56-plan-growth` (plan lines per round, log scale, Steps 5 and 9) | `ch56-anim-stars` (path of 6 becomes a star in 3 rounds; positions schematic) | four-methods table (Step 9) | `code/ch56/output/resolve.txt`, `components.txt` |
| 41 | `ch57-daily` (daily distinct counts, sum, week), `ch57-table` (35-row sketch grid and the answers it gives), `ch57-kll` (KLL rank against value at four ranks) | `ch57-anim-theta` (theta keeps the smallest hashes, the intersection; positions and K schematic) | sketch parameter table (Step 4) | `code/ch57/output/sketches.txt` |
| 42 | `ch58-runs` (rows into the model per action), `ch58-requests` (per-row UDF against mapInPandas; rate per task) | `ch58-anim-retry` (timeout, retry with the same Idempotency-Key, stored answer; speed schematic) | service failure table (Step 6) | `code/ch58/output/inference.txt`, `calling.txt`, `service.py` |
| 43 | `ch59-attempts` (attempt files under local[4,2], row 120 values), `ch59-connect-matrix` (same value in two actions, Classic against Connect), `ch59-fragile` (outputs of the two jobs, clean and chaos) | `ch59-anim-rerun` (a failed attempt leaves no file; append against overwrite on a rerun; timing schematic) | determinism level table (Step 4), save mode table (Step 8) | `code/ch59/output/attempts.txt`, `connect_determinism.txt`, `writes.txt`, `fragile_job.txt` |
| 44 | `ch60-syspath` (worker sys.path, local against cluster), `ch60-zip-archive` (pure and native modules from a zip and an archive) | `ch60-anim-includes` (include list captured once, reused workers keep the path; timing schematic) | interpreter setting table (Step 2) | `code/ch60/output/workers.txt`, `pyfiles.txt` |
| 45 | `ch61-diagnosis` (run time per stage with Python share and flags), `ch61-connect-ops` (sessions, operations, job IDs), `ch61-profiler` (UDF profiler cost) | `ch61-anim-bus` (listener bus fan-out, a full queue drops events; slots and speed schematic) | log format table (Step 11) | `code/ch61/output/eventlog.txt`, `connect_obs.txt`, `profilers.txt`, `LiveListenerBus.scala` |

Every figure was rendered to PNG on a white and on a slate background (`#1e2129`) and read before it was embedded. Every animation was checked on frames during the beats and on its poster frame.

## Totals

- 32 figures (64 SVG files, light and dark), 614 KB in all.
- 13 animations (`book/media/clips/chNN-anim-*.mp4` and `.webp`), 1.97 MB in all. Each mp4 is between about 90 and 200 KB.

## STE

Captions, lead-in sentences and table cells are STE. `scripts/ste-check` gives the same findings before and after the change in each chapter (compared with `git stash`): ch50 2, ch51 1, ch17 1, ch52 0, ch53 13, ch54 0, ch55 6, ch56 23, ch57 0, ch58 5, ch59 15, ch60 5, ch61 1. All are findings of the existing prose. No `--allow` words were added.

## Changes to existing text

- Lists that only compared items became tables, with the same content: ch50 Step 2 (file source options), ch51 Step 2 (read options), ch17 Steps 2 and 8, ch52 Step 3 (commands), ch53 Step 3 (`handleInvalid`), ch54 Step 4, ch56 Step 9, ch57 Step 4, ch58 Step 6, ch59 Steps 4 and 8, ch61 Step 11. New summary tables with no prose removed: ch50 Step 1 (layer contracts), ch60 Step 2 (interpreter settings).
- One sentence of instruction came with a table cell: ch17 Step 8, "Do not use it on a metric view" for `ALTER VIEW ... AS SELECT` (it follows from the result in the same row).
- In ch50 to ch52, the edits are only added figures, tables and captions, as agreed with the writer of ch49.

## Open findings (observed, not changed)

- ch50 Step 7 shows a Python traceback (`IsADirectoryError` in `code/ch50/progress.py`) in place of the progress output, because Spark 4.3 writes the event log as a folder. The script needs to read `eventlog_v2_*/events_*`. The clip `ch50-incremental` shows the intended output. Not changed here: it is a code and output fix, outside this slice.

## Nav and roadmap

No new pages. No nav or roadmap rows.
