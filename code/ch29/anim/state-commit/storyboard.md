# Animation: each batch commits a delta of changed keys; a snapshot holds all keys; a restart loads snapshot plus later deltas

**Claim.** The HDFS-backed state store writes one delta file per version with the new values of the changed keys, a maintenance task writes a snapshot, and a restart rebuilds the latest version from the snapshot and the later deltas. From Chapter 29, Step 3.

**Numbers.** `data.json`: the decoded records of `0/1.delta` to `0/4.delta` and `0/3.snapshot` in `code/ch29/output/c1_hdfs.txt`. Timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `b0` | 0.0 | batches 0 and 1 update the map, two deltas appear | Each batch updates the map and commits one delta file | Each batch updates the map and commits one delta file. | the commit |
| `b2` | 4.8 | batch 2 changes three keys | A delta holds only the changed keys, with their new values | A delta holds only the changed keys, with their new values. | what a delta holds |
| `snap` | 9.6 | snapshot, then batch 3 | The maintenance task writes a snapshot with all keys | The maintenance task writes a snapshot with all keys. | the snapshot |
| `restart` | 14.4 | map clears, snapshot and delta 4 reload it | A restart loads 3.snapshot, then applies 4.delta (stays) | A restart loads 3.snapshot, then applies 4.delta. | the load path |
