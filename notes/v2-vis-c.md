# v2-vis-c: visual aids for chapters 33 to 45

Scope: figures, animations, tables and captions added to existing chapters 33 to 45. The prose substance is not changed. In ch60 to ch62 the edits are only added figures, tables and captions (another writer changes links there).

## How the media are made

- **Figures** are SVG, drawn by `code/chNN/figures/make.py` with the shared kit `code/common/figkit.py` (new helper, used by every chapter of this slice). Each figure has a `-light.svg` and a `-dark.svg` file that differ only in the CSS colour variables, embedded with `#only-light` and `#only-dark`. Run `python3 chNN/figures/make.py <png-dir>` from `code/` to write the SVGs and PNG checks (headless Chrome on white and on the slate background).
- **Animations** are HyperFrames scenes (create-animation skill, theme `paper`, landscape) in `code/chNN/anim/<name>/` with `storyboard.md` and `data.json`. `anim render code/chNN/anim/<name>` renders 1920x1080. The book copy is scaled to 1280x720, H.264 CRF 30, no audio, with a WebP poster: `book/media/clips/chNN-anim-<name>.{mp4,webp}`.
- In each `<figure markdown>`, the two images and the caption are separate paragraphs (blank lines), so `scripts/ste-check` checks the caption as its own sentences.

## Per chapter

| Chapter | Figures | Animation | Tables and other elements | Sources of the numbers |
|---|---|---|---|---|
| 33 | `ch60-routing` (run 1 rows to silver, quarantine or not kept), `ch60-rules` (quarantine counts per rule), `ch60-gate-timeline` (run 2 truncate, failed attempts, skipped gold) | `ch60-anim-routing` | layer contract table (Step 1), file source options table (Step 2) | `code/data/retail/orders/`, `code/ch60/output/two_runs.txt`, `code/ch60/output/gate.txt` |
| 34 | `ch61-three-ways` (append, arrival order, by seq for c-01), `ch61-history` (versions on a seq axis, all columns against TRACK HISTORY ON (city)), `ch61-aux` (targets and auxiliary state tables) | `ch61-anim-scd2` (SCD2 closes and opens versions; the late seq 4 event) | `deduplicationMode`/`computeUpdates` options table (Step 2) | change files in Step 1, `code/ch61/output/drops.txt`, `internals.txt` |
| 35 | `ch23-ratios` (fare per mile by borough, average of ratios against ratio of totals), `ch23-stored` (YAML to column metadata and table properties) | `ch23-anim-measure` (MEASURE() rewrite, two grains, the second aggregation) | YAML key table (Step 2), lifecycle command table (Step 8) | `code/ch23/output/two_errors.txt`, `define.txt`, `grains.txt` |
| 36 | `ch62-five-steps` (StartRun steps, the dry-run line, effects), `ch62-sink-log` (checkpoint versions against the file sink log) | `ch62-anim-flows` (flows of run 2 complete in topological order, at the COMPLETED times) | PipelineCommand table (Step 3) | `PipelineExecution.scala` v4.3.0, Step 8 table, `code/ch62/output/end_to_end.txt` |
| 37 | `ch63-vector` (rate code through StringIndexer, OneHotEncoder, VectorAssembler into 7 named slots), `ch63-mae` (MAE by rate code, both models) | `ch63-anim-treeaggregate` (one treeAggregate pass, then the loop to 11 jobs; partitions and speed schematic) | `handleInvalid` table (Step 3) | `code/ch63/output/estimators.txt`, `fare_pipeline.txt` |
| 38 | `ch65-tokens` (one sentence through the tokenizers and stop words), `ch65-collisions` (words that share a HashingTF slot per numFeatures) | `ch65-anim-lsh` (messages into buckets, candidates, exact distances; buckets schematic) | CountVectorizer against HashingTF table (Step 4) | `code/ch65/output/` (tokenizer, HashingTF and near-duplicate output) |
| 39 | `ch66-tradeoff` (recall against work, BRP LSH and IVF), `ch66-scurve` (banding S-curves from the formula, with measured candidate shares) | `ch66-anim-ivf` (nearest lists light up for nprobe 1 to 16; positions schematic) | | `code/ch66/output/indexes.txt` |
| 40 | `ch67-sweep` (precision and recall per threshold), `ch67-wrong-edge` (one wrong match makes 9 wrong pairs), `ch67-plan-growth` (plan lines per round, log scale, Steps 5 and 9) | `ch67-anim-stars` (path of 6 becomes a star in 3 rounds; positions schematic) | four-methods table (Step 9) | `code/ch67/output/resolve.txt`, `components.txt` |
| 41 | `ch68-daily` (daily distinct counts, sum, week), `ch68-table` (35-row sketch grid and the answers it gives), `ch68-kll` (KLL rank against value at four ranks) | `ch68-anim-theta` (theta keeps the smallest hashes, the intersection; positions and K schematic) | sketch parameter table (Step 4) | `code/ch68/output/sketches.txt` |
| 42 | `ch69-runs` (rows into the model per action), `ch69-requests` (per-row UDF against mapInPandas; rate per task) | `ch69-anim-retry` (timeout, retry with the same Idempotency-Key, stored answer; speed schematic) | service failure table (Step 6) | `code/ch69/output/inference.txt`, `calling.txt`, `service.py` |
| 43 | `ch72-attempts` (attempt files under local[4,2], row 120 values), `ch72-connect-matrix` (same value in two actions, Classic against Connect), `ch72-fragile` (outputs of the two jobs, clean and chaos) | `ch72-anim-rerun` (a failed attempt leaves no file; append against overwrite on a rerun; timing schematic) | determinism level table (Step 4), save mode table (Step 8) | `code/ch72/output/attempts.txt`, `connect_determinism.txt`, `writes.txt`, `fragile_job.txt` |
| 44 | `ch73-syspath` (worker sys.path, local against cluster), `ch73-zip-archive` (pure and native modules from a zip and an archive) | `ch73-anim-includes` (include list captured once, reused workers keep the path; timing schematic) | interpreter setting table (Step 2) | `code/ch73/output/workers.txt`, `pyfiles.txt` |
| 45 | `ch74-diagnosis` (run time per stage with Python share and flags), `ch74-connect-ops` (sessions, operations, job IDs), `ch74-profiler` (UDF profiler cost) | `ch74-anim-bus` (listener bus fan-out, a full queue drops events; slots and speed schematic) | log format table (Step 11) | `code/ch74/output/eventlog.txt`, `connect_obs.txt`, `profilers.txt`, `LiveListenerBus.scala` |

Every figure was rendered to PNG on a white and on a slate background (`#1e2129`) and read before it was embedded. Every animation was checked on frames during the beats and on its poster frame.

## Totals

- 32 figures (64 SVG files, light and dark), 614 KB in all.
- 13 animations (`book/media/clips/chNN-anim-*.mp4` and `.webp`), 1.97 MB in all. Each mp4 is between about 90 and 200 KB.

## STE

Captions, lead-in sentences and table cells are STE. `scripts/ste-check` gives the same findings before and after the change in each chapter (compared with `git stash`): ch60 2, ch61 1, ch23 1, ch62 0, ch63 13, ch65 0, ch66 6, ch67 23, ch68 0, ch69 5, ch72 15, ch73 5, ch74 1. All are findings of the existing prose. No `--allow` words were added.

## Changes to existing text

- Lists that only compared items became tables, with the same content: ch60 Step 2 (file source options), ch61 Step 2 (read options), ch23 Steps 2 and 8, ch62 Step 3 (commands), ch63 Step 3 (`handleInvalid`), ch65 Step 4, ch67 Step 9, ch68 Step 4, ch69 Step 6, ch72 Steps 4 and 8, ch74 Step 11. New summary tables with no prose removed: ch60 Step 1 (layer contracts), ch73 Step 2 (interpreter settings).
- One sentence of instruction came with a table cell: ch23 Step 8, "Do not use it on a metric view" for `ALTER VIEW ... AS SELECT` (it follows from the result in the same row).
- In ch60 to ch62, the edits are only added figures, tables and captions, as agreed with the writer of ch59.

## Open findings (observed, not changed)

- ch60 Step 7 shows a Python traceback (`IsADirectoryError` in `code/ch60/progress.py`) in place of the progress output, because Spark 4.3 writes the event log as a folder. The script needs to read `eventlog_v2_*/events_*`. The clip `ch60-incremental` shows the intended output. Not changed here: it is a code and output fix, outside this slice.

## Nav and roadmap

No new pages. No nav or roadmap rows.
