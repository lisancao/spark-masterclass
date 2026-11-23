# Animation: dynamic partition pruning

**Claim.** A filter on a small dimension (`weekday = 'Sunday'`) becomes a list of join keys at run time. Spark broadcasts that list to the scan of the partitioned fact table, and the scan reads 26 of 181 day partitions. From Chapter 86, Step 4.

**Numbers.** From `code/ch86/output/dpp.txt`: 181 partitions, 26 read with DPP, 22,131,665 rows without and 2,885,119 rows with DPP. The layout of the partitions on screen is schematic (a grid of 181 cells, one per day, January to June 2026); the Sundays are the real Sundays of 2026.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | why this beat |
|---|---|---|---|---|
| `open` | 0.0 | dimension table and the fact grid of 181 days | The fact table has one partition for each day | the input |
| `filter` | 4.5 | the dimension filter keeps the 26 Sundays | The filter on the dimension keeps 26 days | the selective predicate |
| `broadcast` | 9.0 | the 26 keys travel to the scan | Spark broadcasts the join keys to the fact scan | the runtime subquery |
| `prune` | 13.0 | 155 cells dim, 26 stay | The scan reads 26 of 181 partitions | the measured effect |
| `result` | 17.0 | row counts | rows from the scan: 2,885,119 instead of 22,131,665 (stays) | ends on the measured count |
