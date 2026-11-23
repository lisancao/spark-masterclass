# Animation: a recursive CTE runs the anchor, then passes that read only the last pass

**Claim.** `UnionLoop` runs the anchor, then runs the recursive query on the rows of the last pass until a pass returns no rows; each pass is a separate job. From Chapter 20, Step 14.

**Numbers.** The seven zone names from `code/ch20/output/functions.txt`, section 7.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, anchor, recursive query, slot | How a recursive CTE runs (stays) | How a recursive CTE runs. | topic at frame 0 |
| `anchor` | 1.0 | the anchor row moves into the slot and the result | The anchor gives the first row: zone 154 | The anchor gives the first row: zone 154. | start |
| `passes` | 3.4 | each slot row goes into the recursive query; the next zone comes out | Each pass reads only the rows of the last pass | Each pass reads only the rows of the last pass. | the loop |
| `stop` | 8.6 | no rows in the slot | A pass with no rows stops the loop | A pass with no rows stops the loop. | the end condition |
| `result` | 11.0 | holds | Seven rows. Each pass is a separate job. (stays) | Seven rows. Each pass is a separate job. | the cost |
