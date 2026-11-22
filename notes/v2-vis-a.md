# v2-vis-a: visual aids for chapters 2, 3, 5 to 9, 11 to 17

## Scope

Visual aids added to existing chapters. The prose substance is not changed. Changes per chapter: hand-drawn SVG figures (light and dark files, embedded with `#only-light` / `#only-dark`), one HyperFrames animation, and tables, admonitions and code annotations where the text was dense.

## Tooling

- `code/common/svgfig.py`: a small SVG kit. Each figure is drawn once and saved as `book/media/figures/<name>-light.svg` and `<name>-dark.svg`. `check()` renders both into one PNG with headless Chrome for review.
- `code/chNN/figures/make_figures.py`: the figures of a chapter. Run `python3 code/chNN/figures/make_figures.py` to regenerate them.
- `code/chNN/anim/<name>/index.html` and `storyboard.md`: the animation source (create-animation kit, HyperFrames, paper theme). `code/common/build_anim.sh chNN <name>` scaffolds the kit files in a scratch dir, renders landscape, scales to 1280x720 and writes `book/media/clips/chNN-anim-<name>.mp4` and `.webp`.
- Code annotations render only over http (Material's JS), not from `file://`. Checked with `--dump-dom` over a local `http.server`.

## Per chapter

| Chapter | Figures | Animation | Other | Numbers from |
|---|---|---|---|---|
| 2 | `ch02-install-sizes`, `ch02-what-runs-where`, `ch02-data-problems` | `ch02-anim-drivers` (where the driver runs, four installs) | "How to choose" list as a table; annotation on the pip line | chapter table, `code/ch02/output/check_env.txt` |
| 3 | `ch03-cleaning-rules`, `ch03-when-sum` (schematic rows), `ch03-top-zones` | `ch03-anim-plans` (analyzed plans become one optimized plan) | warning admonition for named parameters; annotation on the conditional sum | `code/ch03/output/what_cleaning_drops.txt`, `quickstart.txt` |
| 5 | `ch05-chain-cost`, `ch05-timezones`, `ch05-drift-days` | `ch05-anim-projects` (withColumn loop vs withColumns, CollapseProject) | drift-kind list as a table; TIME warning as an admonition | `code/ch05/output/plans.txt`, `type_system.txt`, `drift.txt` |
| 6 | `ch06-collations`, `ch06-bytes`, `ch06-reject-reasons` | `ch06-anim-ansi` (ANSI stops the job; try_divide gives NULL) | error-method list and collation-name list as tables | `code/ch06/output/ansi.txt`, `strings.txt`, `values.txt` |
| 7 | `ch07-groupby-window`, `ch07-rows-range` | `ch07-anim-window` (Exchange, Sort, Window with running sums) | window-part list, ranking list and window-cost list as tables | `code/ch07/output/windows.txt` |
| 8 | `ch08-script-executions`, `ch08-udf-cost`, `ch08-zone-chain` | `ch08-anim-recursion` (UnionLoop passes on the zone chain) | recursion-limit list as a table | `code/ch08/output/scripting.txt`, `functions.txt` |
| 9 | `ch09-join-types`, `ch09-strategy-times`, `ch09-skew-tasks` | `ch09-anim-movement` (broadcast against sort-merge data movement) | none (the strategy table was already there) | `code/ch09/output/strategies.txt`, `skew.txt` |
| 11 | `ch11-png-header`, `ch11-formats` | `ch11-anim-packing` (2000 small files packed into 63 or 16 tasks) | none | `code/ch11/output/bytes.txt`, `formats.txt` |
