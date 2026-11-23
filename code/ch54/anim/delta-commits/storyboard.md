# Animation: a Delta table changes only by commits that add and remove whole files

**Claim.** WRITE, MERGE and DELETE each write new Parquet files and one JSON commit that adds and removes files. Removed files stay on disk, so only a reader that follows the log sees the right rows. From Chapter 54, Steps 3 and 4.

**Numbers.** `data.json`, from `code/ch54/output/delta_log.txt`: 3, 5 and 4 rows; the MERGE and DELETE metrics; 4 rows as delta and 12 as Parquet. Timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `write` | 0.0 | file A, then 00000.json | A write adds a Parquet file, then creates the commit file | A write adds a Parquet file, then creates the commit file. | the commit order |
| `merge` | 4.8 | file B, 00001.json, A dims | MERGE writes a new file and marks the old file as removed | MERGE writes a new file and marks the old file as removed. | no change in place |
| `delete` | 9.6 | file C, 00002.json, B dims | DELETE copies four rows to remove one: copy-on-write | DELETE copies four rows to remove one: copy-on-write. | the cost |
| `read` | 14.4 | two readers | The log decides what a reader sees: 4 rows, not 12 (stays) | The log decides what a reader sees: 4 rows, not 12. | the trap, with measured counts |
