# v2-vis-b: visual aids for chapters 18 to 31

Scope: figures, animations, tables and admonitions added to the existing chapters 18 to 31. The prose keeps its substance. New sentences and captions are in STE (scripts/ste-check, 0 findings, no new allow words unless listed below).

## How the media are made

- **Figures**: `code/chNN/figures.py` draws each figure with `code/common/figkit.py` (a small SVG builder, shared by these chapters) and writes `book/media/figures/chNN-<name>-light.svg` and `-dark.svg`. The chapter embeds both with `#only-light` and `#only-dark`. Run `cd code && python3 chNN/figures.py`. Every number in a figure comes from `code/chNN/output/`; anything illustrative is labelled schematic in the figure or the caption.
- **Animations**: HyperFrames scenes made with the `create-animation` kit (`anim`), paper theme, landscape, in `code/chNN/anim/<name>/` (index.html, storyboard.md, data.json with the source of each number). Rendered with `anim render <dir> --formats landscape --themes paper`, then scaled to 1280x720 (libx264, crf 28) as `book/media/clips/chNN-anim-<name>.mp4` with a `.webp` poster. `renders/` stays out of git (the kit's .gitignore).
- Each figure was rendered to PNG on a white and on a slate background with headless Chrome and checked by eye before it went into the chapter.

## Per chapter

- ch18: figures ch18-estimates (cost-mode estimates, explain_modes.txt), ch18-optimizer (analyzed vs optimized plan, optimizer.txt), ch18-codegen (codegen stages, explain_modes.txt final plan); animation ch18-anim-coalesce (aqe.txt section 1; partition sizes schematic, 40 of 200 planned tasks drawn); table of the four join runs (aqe.txt). STE 0.
- ch19: figures ch19-rowgroups (file_partitions.txt; equal row-group widths schematic), ch19-coalesce1 (repartition.txt), ch19-memory (memory.txt defaults and the formula), ch19-spill (spill.txt); animation ch19-anim-spill (spill.txt totals; buffer and number of spill files schematic). STE: 0 with the part04 allow list (setting caching repartitioning packing timing cleaning).
- ch20: figures ch20-bench (bench.txt), ch20-calls (calls.txt plan), ch20-batches (arrow_batches.txt); animation ch20-anim-arrow-batches (arrow_batches.txt; timing schematic); code annotations on the bench UDF definitions. STE 0 (part04 allow list).
- ch21: figures ch21-arrow-decision (udtf_core.py order, udtf_core.txt plan nodes), ch21-jsontime (json_table.txt); animation ch21-anim-lifecycle (udtf_core.txt rows and total; timing schematic). Captions with code use figcaption markdown="span" and backticks, so ste-check skips the code. STE 0 (part04 allow list).
- ch22: figures ch22-bytemap (footer.txt offsets, to scale), ch22-skipping (skipping.txt), ch22-codecs (skipping_codecs.txt); animation ch22-anim-skipping (skipping.txt layout C count; file and row-group ranges schematic). STE 0 (part04 list plus skipping).
- ch23: figures ch23-jobs (plans_aqe.txt UI records), ch23-crossover (crossover.txt ratios), ch23-arrowcache (arrow_cache.txt); animation ch23-anim-single-task (plans_aqe.txt; timing schematic). One paragraph in Step 4 split around the jobs figure. STE 0 (part04 allow list).
- ch24: figures ch24-plans (plans-cpu/gluten/comet/rapids/auron.txt, parsed by figures.py), ch24-speedups (bench_table.txt), ch24-memory (bench_table.txt pass 2); animation ch24-anim-transitions (plans-cpu.txt, plans-gluten.txt counts; chain simplified, timing schematic). STE 0 (no extra allow words).
- ch25: figures ch25-windows (s5_windows.txt), ch25-lateness (s5_windows.txt counts), ch25-delay (s6_watermark.txt, s6_delays.txt); animation ch25-anim-watermark (batch table parsed from s6_watermark.txt into data.json; time between batches schematic). STE 0 with the part05 ch25 allow list.
- ch26: figures ch26-routing (m2_parse.txt, m3_dead_letters.txt), ch26-statebounds (m5_state_bounds.txt part 1), ch26-policy (m5_state_bounds.txt part 2); animation ch26-anim-future-event (m4_naive.txt, m4_defended.txt; event positions and timing schematic). STE 1 with the part05 ch26 allow list (the quoted docstring, pre-existing).
- ch27: figures ch27-sessions (t2_sessions.txt), ch27-clock (t3_time.txt); animation ch27-anim-call-order (t2_sessions.txt; new timer from the Sessionizer code; time scale schematic). STE 0 (part05 ch27 allow list).
- ch28: figures ch28-latency (r2_latency.txt medians), ch28-foreach (r3_foreach_default.txt, r3_foreach_64.txt); animation ch28-anim-pipelined (r4_pipeline.txt task counts, r2_latency.txt medians; time scale schematic). STE 0 (part05 ch28 allow list).
- ch29: figures ch29-deltabytes (c1_hdfs.txt sizes, lz4delta.py layout), ch29-versions (c1_hdfs.txt decoded files), ch29-repartition (c3_repartition.txt); animation ch29-anim-state-commit (c1_hdfs.txt records; time scale schematic). STE 0 (part05 ch29 allow list).
- ch30: figures ch30-deltalog (delta_log.txt), ch30-cow-mor (iceberg_log.txt); animation ch30-anim-delta-commits (delta_log.txt; time scale schematic). STE 0.
