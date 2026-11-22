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
| 58 | problems, stream-batches | anim-stream (byte ranges from casefile's own stream reader run on the sample with no Spark, saved as code/ch58/anim/stream_plan.json; row counts agree with demo.txt) | none | 0, 0 (allow: decreasing testing, from part09b notes) |
| 59 | twice, fusion | anim-copy (plan change log from rules.txt, counts from twice.txt) | none | 0, 0 |
| 60 | processes, response, rlimit | anim-frames (frame sizes from tap.txt) | none | 10, 10 |
| 61 | bytes, bench | anim-decode (bytes from layout.txt) | none | 15, 15 |
| 62 | namespaces, boundaries | anim-resolve (results from repro.txt and test_fix.txt) | none | 6, 6 |

Totals: 36 SVG figures (about 239 KB), 17 animations (mp4 plus webp poster, about 3.6 MB in all; each mp4 under 230 KB).

## Sources of the numbers

Every figure script (`code/figures/chNN.py`) and every animation (`code/chNN/anim/<name>/data.json`, `storyboard.md`) names the output file that its numbers come from. Two derived data sets:

- ch50 `anim-deal` and `ch50-balance.svg`: per-file sizes of the 26 sample files that `docs_source.py` copies (computed from `code/data/messy`). The script asserts that the round-robin and largest-first totals equal `docs_source.txt` (22,793 / 10,884 / 30,098 and 21,266 / 21,261 / 21,248).
- ch58 `anim-stream` and `ch58-stream-batches.svg`: byte ranges from `casefile.ChangesStreamReader` run in plain Python on the sample (`code/ch58/anim/stream_plan.json`). Rows per batch (7, 7, 8, 7, 7, 7), total 43 and the end offset of batch 0 agree with `demo.txt`.

Labelled schematic: the ch46 FIFO/FAIR task schedule (end numbers measured), the clip timing of the ch47, ch48, ch51 animations (state times and logs measured), the module order in ch54 `anim-lookup`, the logical plan before pushdown in ch55 `anim-countpush`.

## STE allowlists used (from the part notes)

- ch48: `partitioning pruning coalescing monitoring tuning warning streaming scheduling tracking docstring finding Running` (notes/ch48.md)
- ch50 to ch54: `planning reading attempt listing docstring staging processing programming packaging` (notes/part09a.md)
- ch58: `decreasing testing` (notes/part09b.md)
- others: shared list only. The counts in the table above compare the chapter at commit 3f6ec08 and after, with the same allowlist.

## Open findings and changes to prose

- ch56 Step 11 said "2,000,000 cached binary values". `bench.py` (`N_BLOBS = 20_000_000`) and `bench.txt` ("is_png over 20,000,000 cached values", 6,666,667 matching rows) show 20,000,000. Changed the number in the prose to 20,000,000.
- Rendering note: HyperFrames renders took several minutes each late in the session (load average near 50 from other agents). No effect on output.

## For the integrator

- No nav or roadmap changes. New files: `book/media/figures/ch46-*` to `ch62-*`, `book/media/clips/chNN-anim-*`, `code/figures/`, `code/chNN/anim/`.
- `code/figures/svgkit.py` is a small shared kit; other vis branches may add their own figure code.
