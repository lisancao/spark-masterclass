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
