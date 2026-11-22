# v2-vis-d: visual aids for chapters 46 to 62

Scope: figures, animations, tables and admonitions for the existing chapters 46 to 62. The prose keeps its substance. New text is captions, figure lead-in sentences, tables that replace comparison lists, and "New in 4.3" admonitions.

## How the media is made

- Figures: `code/figures/chNN.py` writes `book/media/figures/chNN-*.svg` with the small kit `code/figures/svgkit.py` (run `cd code/figures && python3 chNN.py`). Transparent background, solid mid-tone boxes with white text, mid-grey text outside boxes, so one file reads in light and dark mode. Each figure was rendered to PNG on a white and on a slate (#1e2129) background with headless Chrome and checked by eye.
- Animations: HyperFrames projects under `code/chNN/anim/<name>/` (create-animation kit, theme `ide`, landscape). Render with `anim render <dir> --formats landscape --themes ide`, then scale to 1280x720 (`ffmpeg -vf scale=1280:720 -crf 28`) into `book/media/clips/chNN-anim-<name>.mp4` with a `.webp` poster from the last frame. Each project has `data.json` (the numbers and where they come from) and `storyboard.md`. Generated files (`renders/`, `theme.*`) are git-ignored.
- STE: captions and new sentences checked with `scripts/ste-check`. Each chapter has no new findings against its count before this work (baseline below).

## Per chapter

| Chapter | Figures | Animation | Other | STE findings (before, after) |
|---|---|---|---|---|
| 46 | session-scope, fifo-fair, first-answer | anim-fifo-fair (schematic schedule, measured end numbers from contention.txt) | patterns table, two New in 4.3 admonitions | 4, 4 |
| 47 | restart-timeline, executor-placement | anim-reconcile (state times from zones-app.txt) | restartConfig table | 11, 11 |
| 48 | gang-timeline, executors-alive | anim-handoff (task times and log lines from dag_submit.txt) | YuniKorn and Volcano table | 0, 0 (chapter allowlist from notes/ch48.md) |
| 49 | checks, average-of-ratios | anim-sieve (outcomes from dryrun.txt) | none | 4, 4 |
| 50 | processes, balance | anim-deal (file sizes of the sample folder, totals from docs_source.txt) | file-opens table | 0, 0 (part09a allowlist) |
| 51 | arrow-timing, key-ranges | anim-requests (request logs from api_source.txt) | pages table | 0, 0 (part09a allowlist) |
| 52 | files-opened, guarded | anim-pushdown (query, pushed filter and counts from files_pushdown.txt) | none | 0, 0 (part09a allowlist) |
| 53 | manifest-states, checkpoint | anim-commit (file names, counts and abort call from writers.txt) | offset designs table | 0, 0 (part09a allowlist) |
| 54 | setups, connect-order | anim-lookup (outcomes of runs 2 and 4 in packaging.txt; module order schematic) | none | 0, 0 (part09a allowlist) |
| 55 | three-plans, partitioning | anim-countpush (final plan and counts from jdocs.txt; logical plan schematic) | timing runs table | 0, 0 |
| 56 | rule-plans, bench | anim-rule (plan lines from planchange.txt, values from extensions.txt) | none; corrected '2,000,000' to '20,000,000' in Step 11 to match bench.py and bench.txt | 0, 0 |
| 57 | any-bytes, plan-rows | anim-plugin (type URLs, rows and error from plugins.txt) | none | 0, 0 |
