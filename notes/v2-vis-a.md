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
| 2 | `ch06-install-sizes`, `ch06-what-runs-where`, `ch06-data-problems` | `ch06-anim-drivers` (where the driver runs, four installs) | "How to choose" list as a table; annotation on the pip line | chapter table, `code/ch06/output/check_env.txt` |
| 3 | `ch07-cleaning-rules`, `ch07-when-sum` (schematic rows), `ch07-top-zones` | `ch07-anim-plans` (analyzed plans become one optimized plan) | warning admonition for named parameters; annotation on the conditional sum | `code/ch07/output/what_cleaning_drops.txt`, `quickstart.txt` |
| 5 | `ch09-chain-cost`, `ch09-timezones`, `ch09-drift-days` | `ch09-anim-projects` (withColumn loop vs withColumns, CollapseProject) | drift-kind list as a table; TIME warning as an admonition | `code/ch09/output/plans.txt`, `type_system.txt`, `drift.txt` |
| 6 | `ch10-collations`, `ch10-bytes`, `ch10-reject-reasons` | `ch10-anim-ansi` (ANSI stops the job; try_divide gives NULL) | error-method list and collation-name list as tables | `code/ch10/output/ansi.txt`, `strings.txt`, `values.txt` |
| 7 | `ch18-groupby-window`, `ch18-rows-range` | `ch18-anim-window` (Exchange, Sort, Window with running sums) | window-part list, ranking list and window-cost list as tables | `code/ch18/output/windows.txt` |
| 8 | `ch20-script-executions`, `ch20-udf-cost`, `ch20-zone-chain` | `ch20-anim-recursion` (UnionLoop passes on the zone chain) | recursion-limit list as a table | `code/ch20/output/scripting.txt`, `functions.txt` |
| 9 | `ch11-join-types`, `ch11-strategy-times`, `ch11-skew-tasks` | `ch11-anim-movement` (broadcast against sort-merge data movement) | none (the strategy table was already there) | `code/ch11/output/strategies.txt`, `skew.txt` |
| 11 | `ch31-png-header`, `ch31-formats` | `ch31-anim-packing` (2000 small files packed into 63 or 16 tasks) | none | `code/ch31/output/bytes.txt`, `formats.txt` |
| 12 | `ch32-multiline-events`, `ch32-dropmalformed` | `ch32-anim-positions` (one header, columns by position, then unionByName) | none (the option table was already there) | `code/data/messy/logs/app.log`, `code/ch32/output/csv_drift.txt` |
| 13 | `ch33-infer-cost`, `ch33-type-merge`, `ch33-balance` | `ch33-anim-inference` (two passes without a schema, one with) | none (the mode table was already there) | `code/ch33/output/infer_cost.txt`, `infer.txt`, `drift.txt` |
| 14 | `ch34-explode`, `ch34-three-valued` | `ch34-anim-tracing` (a Python lambda traced into a Catalyst expression) | aggregate-trap list and codegen list as tables | `code/ch34/output/nested.txt`, `lambdas.txt` |
| 15 | `ch35-key-profile`, `ch35-shredding`, `ch35-extraction-cost` | `ch35-anim-shredding` (fields into typed columns, one-column read) | none (the comparison table was already there) | `code/ch35/output/land.txt`, `shred.txt`, `speed.txt`, `code/data/messy/events` |
| 16 | `ch36-confluent`, `ch36-record-bytes` | `ch36-anim-wrongschema` (Avro bytes decoded with the right and a wrong schema) | none (the format table was already there) | `code/ch36/output/avro.txt`, `protobuf.txt` |
| 17 | `ch37-bgr`, `ch37-parquet-blobs` | `ch37-anim-forget` (delete, rewrite, expire on snapshots and files) | the delete-sequence list as a table | `code/ch37/output/media.txt`, `blobs.txt` |

Totals: 36 figures (72 SVG files, light and dark, 0.50 MB), 14 animations (mp4 plus webp poster, 3.49 MB; the largest mp4 is about 280 KB). Media added: about 4.0 MB.

## Claims and sources

Every number in a figure or an animation comes from the chapter's saved output (`code/chNN/output/`) or from the chapter's own tables, as listed in the "Numbers from" column above and in each `code/chNN/anim/<name>/storyboard.md`. Anything illustrative is labelled on the figure or on screen as schematic: example rows (ch07 conditional sums), dot positions and partitions (ch11, ch33), the squares that stand for files (ch31), the six drawn `withColumn` calls (ch09) and the row values of the ANSI animation (ch10). Two derived values: the PNG chunk length `00 00 00 0D` and the width bytes `00 00 00 20` in `ch31-png-header` follow from the PNG format and the decoded width 32; the four unread bytes in `ch36-anim-wrongschema` follow from the Avro byte layout and the decoded row.

## Environment

- Figures: `code/common/svgfig.py` (Python 3.12, no extra packages), checked with Google Chrome headless.
- Animations: create-animation kit, HyperFrames backend, deck theme `paper`, landscape 1920x1080 scaled with ffmpeg (libx264, CRF 26) to 1280x720, webp poster from the last second.
- No Spark runs were needed: all numbers come from saved output.

## Open findings (text against saved output, not changed)

- **ch10, Step 2:** the text says "Seven queries return NULL, and two return a wrong number". `code/ch10/output/ansi.txt` shows eight `None` results and two wrong numbers (ten queries).
- **ch11, Step 3:** the text quotes 0.16 s, 0.56 s, 0.73 s and 8.50 s. `code/ch11/output/strategies.txt` shows 0.17 s, 0.54 s, 0.75 s and 8.61 s. The figure `ch11-strategy-times` uses the saved output.
- **Possible merge overlap:** `code/common/svgfig.py` and `code/common/build_anim.sh` are new shared files. If another visual-aids branch adds files with the same names, keep one copy.

## STE

`scripts/ste-check` on each changed chapter, with the part allow lists (`notes/part02.md` words for ch06 to ch11, `notes/.ste-allow-part03` plus the part03 words for ch31 to ch37). No chapter has a new finding:

| Chapter | Findings before | Findings after |
|---|---|---|
| 2, 3, 5, 6, 7, 8, 9 | 0 | 0 |
| 11, 13, 14, 15, 16, 17 | 0 | 0 |
| 12 | 1 (`is parsed`, already in the chapter) | 1 (the same) |

(ch09 without the allow list has the same 4 findings before and after.) Rules for captions: alt text short, a blank line between the two images and before `<figcaption>` (the checker joins lines of one paragraph into one sentence), captions in STE description text.

## Nav and roadmap

No new chapters, so no nav entries or roadmap rows.
