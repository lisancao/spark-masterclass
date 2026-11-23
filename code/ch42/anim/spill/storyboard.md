# Animation: a sort task spills sorted runs to disk when its memory share is full, then merges them

**Claim.** A sort that needs more execution memory than its share writes sorted runs to the local disk, frees the memory, and merges the runs at the end. From Chapter 42, Step 7.

**Numbers.** `data.json`: 2 sort tasks, 370 MiB memory spilled, 98 MiB disk spilled (`code/ch42/output/spill.txt`). Buffer size, timing and the number of spill files are schematic and labelled.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | rows stream in, buffer fills | Rows arrive, and the sort keeps them in its buffer | Rows arrive, and the sort keeps them in its buffer. | sets up the buffer |
| `spill1` | 4.8 | full buffer drops a run to disk, refills | When no more memory comes, it writes a sorted run to disk | When no more memory comes, it writes a sorted run to disk. | the spill itself |
| `spill2` | 9.6 | second run, last run stays in memory | It frees the memory and continues with the next rows | It frees the memory and continues with the next rows. | spill repeats, nothing fails |
| `merge` | 14.4 | runs merge into the output; totals | At the end, the task merges all runs into one sorted output (stays) | At the end, the task merges all runs into one sorted output. | the cost, with measured totals |
