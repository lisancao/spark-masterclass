# v2-vis-c: visual aids for chapters 33 to 45

Scope: figures, animations, tables and captions added to existing chapters 33 to 45. The prose substance is not changed. In ch33 to ch36 the edits are only added figures, tables and captions (another writer changes links there).

## How the media are made

- **Figures** are SVG, drawn by `code/chNN/figures/make.py` with the shared kit `code/common/figkit.py` (new helper, used by every chapter of this slice). Each figure has a `-light.svg` and a `-dark.svg` file that differ only in the CSS colour variables, embedded with `#only-light` and `#only-dark`. Run `python3 chNN/figures/make.py <png-dir>` from `code/` to write the SVGs and PNG checks (headless Chrome on white and on the slate background).
- **Animations** are HyperFrames scenes (create-animation skill, theme `paper`, landscape) in `code/chNN/anim/<name>/` with `storyboard.md` and `data.json`. `anim render code/chNN/anim/<name>` renders 1920x1080. The book copy is scaled to 1280x720, H.264 CRF 30, no audio, with a WebP poster: `book/media/clips/chNN-anim-<name>.{mp4,webp}`.
- In each `<figure markdown>`, the two images and the caption are separate paragraphs (blank lines), so `scripts/ste-check` checks the caption as its own sentences.

## Per chapter

| Chapter | Figures | Animation | Tables and other elements | Sources of the numbers |
|---|---|---|---|---|
| 33 | `ch33-routing` (run 1 rows to silver, quarantine or not kept), `ch33-rules` (quarantine counts per rule), `ch33-gate-timeline` (run 2 truncate, failed attempts, skipped gold) | `ch33-anim-routing` | layer contract table (Step 1), file source options table (Step 2) | `code/data/retail/orders/`, `code/ch33/output/two_runs.txt`, `code/ch33/output/gate.txt` |
| 34 | `ch34-three-ways` (append, arrival order, by seq for c-01), `ch34-history` (versions on a seq axis, all columns against TRACK HISTORY ON (city)), `ch34-aux` (targets and auxiliary state tables) | `ch34-anim-scd2` (SCD2 closes and opens versions; the late seq 4 event) | `deduplicationMode`/`computeUpdates` options table (Step 2) | change files in Step 1, `code/ch34/output/drops.txt`, `internals.txt` |
| 35 | `ch35-ratios` (fare per mile by borough, average of ratios against ratio of totals), `ch35-stored` (YAML to column metadata and table properties) | `ch35-anim-measure` (MEASURE() rewrite, two grains, the second aggregation) | YAML key table (Step 2), lifecycle command table (Step 8) | `code/ch35/output/two_errors.txt`, `define.txt`, `grains.txt` |
| 36 | `ch36-five-steps` (StartRun steps, the dry-run line, effects), `ch36-sink-log` (checkpoint versions against the file sink log) | `ch36-anim-flows` (flows of run 2 complete in topological order, at the COMPLETED times) | PipelineCommand table (Step 3) | `PipelineExecution.scala` v4.3.0, Step 8 table, `code/ch36/output/end_to_end.txt` |
| 37 | `ch37-vector` (rate code through StringIndexer, OneHotEncoder, VectorAssembler into 7 named slots), `ch37-mae` (MAE by rate code, both models) | `ch37-anim-treeaggregate` (one treeAggregate pass, then the loop to 11 jobs; partitions and speed schematic) | `handleInvalid` table (Step 3) | `code/ch37/output/estimators.txt`, `fare_pipeline.txt` |
| 38 | `ch38-tokens` (one sentence through the tokenizers and stop words), `ch38-collisions` (words that share a HashingTF slot per numFeatures) | `ch38-anim-lsh` (messages into buckets, candidates, exact distances; buckets schematic) | CountVectorizer against HashingTF table (Step 4) | `code/ch38/output/` (tokenizer, HashingTF and near-duplicate output) |
| 39 | `ch39-tradeoff` (recall against work, BRP LSH and IVF), `ch39-scurve` (banding S-curves from the formula, with measured candidate shares) | `ch39-anim-ivf` (nearest lists light up for nprobe 1 to 16; positions schematic) | | `code/ch39/output/indexes.txt` |
