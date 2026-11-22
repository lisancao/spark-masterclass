# v2-vis-b: visual aids for chapters 18 to 31

Scope: figures, animations, tables and admonitions added to the existing chapters 18 to 31. The prose keeps its substance. New sentences and captions are in STE (scripts/ste-check, 0 findings, no new allow words unless listed below).

## How the media are made

- **Figures**: `code/chNN/figures.py` draws each figure with `code/common/figkit.py` (a small SVG builder, shared by these chapters) and writes `book/media/figures/chNN-<name>-light.svg` and `-dark.svg`. The chapter embeds both with `#only-light` and `#only-dark`. Run `cd code && python3 chNN/figures.py`. Every number in a figure comes from `code/chNN/output/`; anything illustrative is labelled schematic in the figure or the caption.
- **Animations**: HyperFrames scenes made with the `create-animation` kit (`anim`), paper theme, landscape, in `code/chNN/anim/<name>/` (index.html, storyboard.md, data.json with the source of each number). Rendered with `anim render <dir> --formats landscape --themes paper`, then scaled to 1280x720 (libx264, crf 28) as `book/media/clips/chNN-anim-<name>.mp4` with a `.webp` poster. `renders/` stays out of git (the kit's .gitignore).
- Each figure was rendered to PNG on a white and on a slate background with headless Chrome and checked by eye before it went into the chapter.

## Per chapter

- ch18: figures ch18-estimates (cost-mode estimates, explain_modes.txt), ch18-optimizer (analyzed vs optimized plan, optimizer.txt), ch18-codegen (codegen stages, explain_modes.txt final plan); animation ch18-anim-coalesce (aqe.txt section 1; partition sizes schematic, 40 of 200 planned tasks drawn); table of the four join runs (aqe.txt). STE 0.
- ch19: figures ch19-rowgroups (file_partitions.txt; equal row-group widths schematic), ch19-coalesce1 (repartition.txt), ch19-memory (memory.txt defaults and the formula), ch19-spill (spill.txt); animation ch19-anim-spill (spill.txt totals; buffer and number of spill files schematic). STE: 0 with the part04 allow list (setting caching repartitioning packing timing cleaning).
