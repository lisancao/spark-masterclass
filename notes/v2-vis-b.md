# v2-vis-b: visual aids for chapters 18 to 31

Scope: figures, animations, tables and admonitions added to the existing chapters 18 to 31. The prose keeps its substance. New sentences and captions are in STE (scripts/ste-check, 0 findings, no new allow words unless listed below).

## How the media are made

- **Figures**: `code/chNN/figures.py` draws each figure with `code/common/figkit.py` (a small SVG builder, shared by these chapters) and writes `book/media/figures/chNN-<name>-light.svg` and `-dark.svg`. The chapter embeds both with `#only-light` and `#only-dark`. Run `cd code && python3 chNN/figures.py`. Every number in a figure comes from `code/chNN/output/`; anything illustrative is labelled schematic in the figure or the caption.
- **Animations**: HyperFrames scenes made with the `create-animation` kit (`anim`), paper theme, landscape, in `code/chNN/anim/<name>/` (index.html, storyboard.md, data.json with the source of each number). Rendered with `anim render <dir> --formats landscape --themes paper`, then scaled to 1280x720 (libx264, crf 28) as `book/media/clips/chNN-anim-<name>.mp4` with a `.webp` poster. `renders/` stays out of git (the kit's .gitignore).
- Each figure was rendered to PNG on a white and on a slate background with headless Chrome and checked by eye before it went into the chapter.

## Per chapter

- ch32: figures ch32-estimates (cost-mode estimates, explain_modes.txt), ch32-optimizer (analyzed vs optimized plan, optimizer.txt), ch32-codegen (codegen stages, explain_modes.txt final plan); animation ch32-anim-coalesce (aqe.txt section 1; partition sizes schematic, 40 of 200 planned tasks drawn); table of the four join runs (aqe.txt). STE 0.
- ch33: figures ch33-rowgroups (file_partitions.txt; equal row-group widths schematic), ch33-coalesce1 (repartition.txt), ch33-memory (memory.txt defaults and the formula), ch33-spill (spill.txt); animation ch33-anim-spill (spill.txt totals; buffer and number of spill files schematic). STE: 0 with the part04 allow list (setting caching repartitioning packing timing cleaning).
- ch34: figures ch34-bench (bench.txt), ch34-calls (calls.txt plan), ch34-batches (arrow_batches.txt); animation ch34-anim-arrow-batches (arrow_batches.txt; timing schematic); code annotations on the bench UDF definitions. STE 0 (part04 allow list).
- ch35: figures ch35-arrow-decision (udtf_core.py order, udtf_core.txt plan nodes), ch35-jsontime (json_table.txt); animation ch35-anim-lifecycle (udtf_core.txt rows and total; timing schematic). Captions with code use figcaption markdown="span" and backticks, so ste-check skips the code. STE 0 (part04 allow list).
- ch36: figures ch36-bytemap (footer.txt offsets, to scale), ch36-skipping (skipping.txt), ch36-codecs (skipping_codecs.txt); animation ch36-anim-skipping (skipping.txt layout C count; file and row-group ranges schematic). STE 0 (part04 list plus skipping).
- ch37: figures ch37-jobs (plans_aqe.txt UI records), ch37-crossover (crossover.txt ratios), ch37-arrowcache (arrow_cache.txt); animation ch37-anim-single-task (plans_aqe.txt; timing schematic). One paragraph in Step 4 split around the jobs figure. STE 0 (part04 allow list).
- ch38: figures ch38-plans (plans-cpu/gluten/comet/rapids/auron.txt, parsed by figures.py), ch38-speedups (bench_table.txt), ch38-memory (bench_table.txt pass 2); animation ch38-anim-transitions (plans-cpu.txt, plans-gluten.txt counts; chain simplified, timing schematic). STE 0 (no extra allow words).
- ch39: figures ch39-windows (s5_windows.txt), ch39-lateness (s5_windows.txt counts), ch39-delay (s6_watermark.txt, s6_delays.txt); animation ch39-anim-watermark (batch table parsed from s6_watermark.txt into data.json; time between batches schematic). STE 0 with the part05 ch39 allow list.
- ch40: figures ch40-routing (m2_parse.txt, m3_dead_letters.txt), ch40-statebounds (m5_state_bounds.txt part 1), ch40-policy (m5_state_bounds.txt part 2); animation ch40-anim-future-event (m4_naive.txt, m4_defended.txt; event positions and timing schematic). STE 1 with the part05 ch40 allow list (the quoted docstring, pre-existing).
- ch41: figures ch41-sessions (t2_sessions.txt), ch41-clock (t3_time.txt); animation ch41-anim-call-order (t2_sessions.txt; new timer from the Sessionizer code; time scale schematic). STE 0 (part05 ch41 allow list).
- ch42: figures ch42-latency (r2_latency.txt medians), ch42-foreach (r3_foreach_default.txt, r3_foreach_64.txt); animation ch42-anim-pipelined (r4_pipeline.txt task counts, r2_latency.txt medians; time scale schematic). STE 0 (part05 ch42 allow list).
- ch43: figures ch43-deltabytes (c1_hdfs.txt sizes, lz4delta.py layout), ch43-versions (c1_hdfs.txt decoded files), ch43-repartition (c3_repartition.txt); animation ch43-anim-state-commit (c1_hdfs.txt records; time scale schematic). STE 0 (part05 ch43 allow list).
- ch44: figures ch44-deltalog (delta_log.txt), ch44-cow-mor (iceberg_log.txt); animation ch44-anim-delta-commits (delta_log.txt; time scale schematic). STE 0.
- ch45: figures ch45-schemas (evolve.txt), ch45-pruning (evolve.txt, tables.txt), ch45-maintenance (maintenance.txt); animation ch45-anim-snapshot-commit (tables.txt files, sizes and rows; time scale schematic). STE 0.
- ch35 (second commit): the DataFrame and SQL forms of the lateral call in Step 5 are now `=== "DataFrame"` / `=== "SQL"` tabs; the SQL tab shows the `spark.udtf.register("pairs", Pairs)` line from `udtf_core.py`.

## Totals

- 39 figures (78 SVG files, light and dark), about 0.64 MB.
- 14 animations (mp4 1280x720 plus webp poster), about 3.6 MB together; the largest mp4 is about 320 KB.
- Each chapter commit passed `mkdocs build --strict`.

## Conventions for the integrator

- Captions use `<figcaption markdown="span">` so that backticks render as code. `ste_prep.py` strips HTML tags, so a word inside `<code>` is checked as prose; backticks avoid that.
- Figure alt text counts toward the sentence length in `ste-check` (the alt is in both images), so alt texts are short.
- The part05 allow lists work only when each word is a separate argument. A shell variable that holds the list is one argument in zsh, and then the counts look higher than they are.
- No nav or roadmap changes: no new pages.
- Each animation directory (`code/chNN/anim/<name>/`) has `storyboard.md` (beats, captions as the narration script) and `data.json` (the source file of each number). `theme.*` and `renders/` are ignored by the kit's `.gitignore`. Rebuild: `anim render code/chNN/anim/<name> --formats landscape --themes paper`, then scale to 1280x720 with ffmpeg (libx264, crf 28, no audio) and take a webp poster from a late frame.

## Open findings

- None about Spark. Nothing was rerun; every number comes from the existing `code/chNN/output/` files.
