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
