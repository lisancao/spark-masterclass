# Animation: a task retry leaves no duplicate, a rerun with append does

**Claim.** The commit protocol drops a failed attempt, so a retry is safe; a rerun of the whole job appends a second copy unless the write replaces (Chapter 43, Step 8).

**Numbers.** `data.json`, from `code/ch43/output/writes.txt` (append run twice: 20 rows, 10 expected; overwrite: 10). Timing is schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, attempt 0 | A retry is safe. A rerun is not (stays) | Two kinds of re-execution. | subject at frame 0 |
| `retry` | 0.6 | attempt 0 fails, attempt 1 commits | A failed attempt leaves no file in the output | A failed attempt leaves no file in the output. | the commit protocol |
| `run1` | 3.0 | both folders get 10 rows | (same caption) | | the first run |
| `rerun` | 4.8 | append adds 10, overwrite replaces 10 | The commit makes one job atomic, not a second run | But a second run is a new job. | the duplicate write |
| `result` | 9.2 | hold | append: 20 rows, 10 expected. overwrite: 10 rows. (stays) | Append doubles the rows. Overwrite does not. | measured result |
