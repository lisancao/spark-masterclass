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
